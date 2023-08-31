from typing import List


class CensusDatasetSchema:
    def __init__(self):
        self.company = 'company'
        self.company_public_response = 'company_public_response'
        self.company_response = 'company_response'
        self.complaint_id = 'complaint_id'
        self.complaint_what_happened = 'complaint_what_happened'
        self.consumer_consent_provided = 'consumer_consent_provided'
        self.consumer_disputed = 'consumer_disputed'
        self.date_received = 'date_received'
        self.date_sent_to_company = 'date_sent_to_company'
        self.issue = 'issue'
        self.product = 'product'
        self.state = 'state'
        self.sub_issue = 'sub_issue'
        self.sub_product = 'sub_product'
        self.submitted_via = 'submitted_via'
        self.tags = 'tags'
        self.timely = 'timely'
        self.zip_code = 'zip_code'
        self.days_to_forward_complaint = "days_to_forward_complaint"

    
    
    @property
    def target_value(self)->List[str]:
        return ['Yes','No']

    @property
    def one_hot_features(self) -> List[str]:
        return [self.consumer_consent_provided, self.timely, self.state]

    @property
    def binary_features(self) -> List[str]:
        return [self.product, self.submitted_via, self.company_response]


    @property
    def derived_input_features(self) -> List[str]:
        return [self.date_sent_to_company, self.date_received]

    @property
    def text_feature(self) -> str:
        return self.issue

    @property
    def derived_output_feature(self) -> str:
        return self.days_to_forward_complaint

    @property
    def target_column(self) -> str:
        return self.consumer_disputed

    @staticmethod
    def transformed_name(column: str) -> str:
        return f"{column}_xf"

    @staticmethod
    def transformed_names(columns: List[str]) -> List[str]:
        transformed_columns = []
        for column in columns:
            transformed_columns.append(CensusDatasetSchema.transformed_name(column=column))
        return transformed_columns

from tensorflow.python.framework.ops import Tensor
from tensorflow.python.framework.sparse_tensor import SparseTensor


from dataclasses import dataclass

class DataType:
    company:Tensor
    company_public_response:SparseTensor
    company_response:Tensor
    complaint_id:Tensor
    complaint_what_happened:Tensor
    consumer_consent_provided:SparseTensor
    consumer_disputed:Tensor
    date_received:Tensor
    date_sent_to_company:Tensor
    issue:Tensor
    product:Tensor
    state:SparseTensor
    sub_issue:SparseTensor
    sub_product:SparseTensor
    submitted_via:Tensor



# company <class 'tensorflow.python.framework.ops.Tensor'>
# company_public_response <class 'tensorflow.python.framework.sparse_tensor.SparseTensor'>
# company_response <class 'tensorflow.python.framework.ops.Tensor'>
# complaint_id <class 'tensorflow.python.framework.ops.Tensor'>
# complaint_what_happened <class 'tensorflow.python.framework.ops.Tensor'>
# consumer_consent_provided <class 'tensorflow.python.framework.sparse_tensor.SparseTensor'>
# consumer_disputed <class 'tensorflow.python.framework.ops.Tensor'>
# date_received <class 'tensorflow.python.framework.ops.Tensor'>
# date_sent_to_company <class 'tensorflow.python.framework.ops.Tensor'>
# issue <class 'tensorflow.python.framework.ops.Tensor'>
# product <class 'tensorflow.python.framework.ops.Tensor'>
# state <class 'tensorflow.python.framework.sparse_tensor.SparseTensor'>
# sub_issue <class 'tensorflow.python.framework.sparse_tensor.SparseTensor'>
# sub_product <class 'tensorflow.python.framework.sparse_tensor.SparseTensor'>
# submitted_via <class 'tensorflow.python.framework.ops.Tensor'>