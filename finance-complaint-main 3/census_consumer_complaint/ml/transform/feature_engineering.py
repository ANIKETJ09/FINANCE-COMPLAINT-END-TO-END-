from census_consumer_complaint.config.schema import CensusDatasetSchema
import tensorflow as tf
import tensorflow_transform as tft





def preprocessing_fn(inputs):
    try:
        print(inputs)
        print(type(inputs))
        for column in inputs.keys():
            print(column,type(inputs[column]))
        outputs = {}
        schema = CensusDatasetSchema()
  
        outputs[CensusDatasetSchema.transformed_name(column=schema.text_feature)] = tft.compute_and_apply_vocabulary(inputs[schema.text_feature],num_oov_buckets=10)
        



        one_hot_feature = schema.one_hot_features
        transformed_one_hot_feature = CensusDatasetSchema.transformed_names(columns=one_hot_feature)
        
        for input_col,tf_input_col in zip(one_hot_feature,transformed_one_hot_feature):
             outputs[tf_input_col] = tft.compute_and_apply_vocabulary(inputs[input_col],num_oov_buckets=10)


        binary_encoder_feature = schema.binary_features
        tf_binary_encoder_feature = CensusDatasetSchema.transformed_names(columns=binary_encoder_feature)
        
        # #applying bag of word
        for input_col,tf_input_col in zip(binary_encoder_feature,tf_binary_encoder_feature):
             outputs[tf_input_col] = tft.compute_and_apply_vocabulary(inputs[input_col],num_oov_buckets=10)
        outputs[CensusDatasetSchema.transformed_name(column=schema.target_column)] = inputs[schema.target_column]
        return outputs
    except Exception as e:
        raise e
    