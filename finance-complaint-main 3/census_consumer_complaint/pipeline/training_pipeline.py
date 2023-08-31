from tfx.components.base import executor_spec
from tfx.components.example_gen.custom_executors import parquet_executor
from tfx.components import FileBasedExampleGen
from census_consumer_complaint.config.configuration import CensusConsumerConfiguration
from tfx.components.base.base_component import BaseComponent
from tfx.components import SchemaGen,StatisticsGen,ExampleValidator,Transform,Trainer
from census_consumer_complaint.exception.exception import CensusConsumerException
from typing import List
from tfx.proto import trainer_pb2
import os,sys
from tfx.dsl.components.base import executor_spec
from tfx.components import Evaluator
from tfx.v1.dsl import Resolver, Channel
from tfx.v1.types.standard_artifacts import Model, ModelBlessing
from census_consumer_complaint.config.schema import CensusDatasetSchema
from tfx.v1.dsl import experimental
from tfx.components import Pusher
from tfx.proto import pusher_pb2
import tensorflow_model_analysis as tfma

schema = CensusDatasetSchema()
TRAINING_STEPS = 1000
EVALUATION_STEPS = 100

class CensusConsumerTrainingPipeline:

    def __init__(self,census_consumer_config:CensusConsumerConfiguration=CensusConsumerConfiguration()) -> None:
        self.census_consumer_config=census_consumer_config

    def get_pipeline_component(self):
        try:

            #data ingestion
            example_gen = FileBasedExampleGen(
            input_base=self.census_consumer_config.feature_store_dir,
            #custom_executor_spec=executor_spec.ExecutorClassSpec(parquet_executor.Executor)
            custom_executor_spec=executor_spec.BeamExecutorSpec(parquet_executor.Executor)
            )

            #data validation

            statistic_gen = StatisticsGen(examples=example_gen.outputs['examples'])
            schema_gen  = SchemaGen(statistics=statistic_gen.outputs['statistics'])
            example_val = ExampleValidator(schema=schema_gen.outputs['schema'],statistics=statistic_gen.outputs['statistics'])


            #data transformation
            transform = Transform(examples=example_gen.outputs['examples'],
            schema=schema_gen.outputs['schema'],
            module_file=self.census_consumer_config.transform_module_file_path)

            #model training

            training_kwargs = {
                "module_file": self.census_consumer_config.trainer_module_file_path,
                "examples":transform.outputs['transformed_examples'],
                "schema": schema_gen.outputs['schema'],
                "transform_graph":transform.outputs['transform_graph'],
                "train_args":trainer_pb2.TrainArgs(num_steps = TRAINING_STEPS),
                "eval_args":trainer_pb2.EvalArgs(num_steps =EVALUATION_STEPS)

            }
            trainer = Trainer(**training_kwargs)

            #model evaluation

            eval_config = tfma.EvalConfig(
            model_specs=[
                tfma.ModelSpec(
                    signature_name="serving_default",
                    label_key=schema.target_column,

                )
            ],
            slicing_specs=[tfma.SlicingSpec(), tfma.SlicingSpec(feature_keys=[schema.product])],
            metrics_specs=[
                tfma.MetricsSpec(
                    metrics=[
                        tfma.MetricConfig(
                            class_name="BinaryAccuracy",
                            threshold=tfma.MetricThreshold(
                                value_threshold=tfma.GenericValueThreshold(
                                    lower_bound={"value": 0.5}
                                ),
                                change_threshold=tfma.GenericChangeThreshold(
                                    direction=tfma.MetricDirection.HIGHER_IS_BETTER,
                                    absolute={"value": -1e-10},
                                ),
                            ),
                        ),
                        tfma.MetricConfig(class_name="Precision"),
                        tfma.MetricConfig(class_name="Recall"),
                        tfma.MetricConfig(class_name="ExampleCount"),
                        tfma.MetricConfig(class_name="AUC"),
                            ],
                        )
                    ],
                )

            model_resolver = Resolver(
                                    strategy_class=experimental.LatestBlessedModelStrategy,
                                    model=Channel(type=Model),
                                    model_blessing=Channel(type=ModelBlessing),
                                    )

            evaluator = Evaluator(
            examples=example_gen.outputs['examples'],
            model=trainer.outputs['model'],
            eval_config=eval_config,
            baseline_model=model_resolver.outputs['model'], )

            pusher = Pusher(
                    model=trainer.outputs['model'],
                    model_blessing=evaluator.outputs['blessing'],
                    push_destination=pusher_pb2.PushDestination(
                        filesystem=pusher_pb2.PushDestination.Filesystem(
                            base_directory=self.census_consumer_config.serving_model_dir )), )
            return [example_gen,statistic_gen,schema_gen,example_val,transform,trainer,model_resolver,evaluator,pusher]
        except Exception as e:
            raise CensusConsumerException(e,sys)