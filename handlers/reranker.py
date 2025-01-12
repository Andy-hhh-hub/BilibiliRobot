#!/usr/bin/env python
# -*- coding: UTF-8 -*-
# ====================================
# @Project ：BilibiliRobot
# @IDE     ：PyCharm
# @Author  ：Huang Andy Hong Hua
# @Email   ：
# @Date    ：2024/3/20 10:33
# ====================================
import numpy as np
from typing import Any, Dict, List, Optional, Tuple
from pydantic import BaseModel, Extra, root_validator
from langchain.llms.sagemaker_endpoint import ContentHandlerBase
from langchain.docstore.document import Document


def norm(scores):
    # 将数据标准化到 (-pi/2, pi/2) 区间
    max_val = max(scores)
    min_val = min(scores)
    scaled_data = [(x - min_val) / (max_val - min_val + 1e-5) * np.pi - np.pi / 2 for x in scores]

    # 将标准化后的数据应用 ArcTan 函数
    atan_values = [(np.arctan(x) + np.pi / 2) / np.pi for x in scaled_data]
    return atan_values


class SagemakerEndpointBgeReRanker(BaseModel):
    """Wrapper around custom Sagemaker Inference Endpoints.

    To use, you must supply the endpoint name from your deployed
    Sagemaker model & the region where it is deployed.

    To authenticate, the AWS client uses the following methods to
    automatically load credentials:
    https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html

    If a specific credential profile should be used, you must pass
    the name of the profile from the ~/.aws/credentials file that is to be used.

    Make sure the credentials / roles used have the required policies to
    access the Sagemaker endpoint.
    See: https://docs.aws.amazon.com/IAM/latest/UserGuide/access_policies.html
    """

    """
    Example:
        .. code-block:: python

            from langchain.embeddings import SagemakerEndpointEmbeddings
            endpoint_name = (
                "my-endpoint-name"
            )
            region_name = (
                "us-west-2"
            )
            credentials_profile_name = (
                "default"
            )
            se = SagemakerEndpointEmbeddings(
                endpoint_name=endpoint_name,
                region_name=region_name,
                credentials_profile_name=credentials_profile_name
            )
    """
    client: Any  #: :meta private:

    endpoint_name: str = ""
    """The name of the endpoint from the deployed Sagemaker model.
    Must be unique within an AWS Region."""

    region_name: str = ""
    """The aws region where the Sagemaker model is deployed, eg. `us-west-2`."""

    credentials_profile_name: Optional[str] = None
    """The name of the profile in the ~/.aws/credentials or ~/.aws/config files, which
    has either access keys or role information specified.
    If not specified, the default credential profile or, if on an EC2 instance,
    credentials from IMDS will be used.
    See: https://boto3.amazonaws.com/v1/documentation/api/latest/guide/credentials.html
    """

    content_handler: ContentHandlerBase
    """The content handler class that provides an input and
    output transform functions to handle formats between LLM
    and the endpoint.
    """

    """
     Example:
        .. code-block:: python

        from langchain.llms.sagemaker_endpoint import ContentHandlerBase

        class ContentHandler(ContentHandlerBase):
                content_type = "application/json"
                accepts = "application/json"

                def transform_input(self, prompt: str, model_kwargs: Dict) -> bytes:
                    input_str = json.dumps({prompt: prompt, **model_kwargs})
                    return input_str.encode('utf-8')

                def transform_output(self, output: bytes) -> str:
                    response_json = json.loads(output.read().decode("utf-8"))
                    return response_json[0]["generated_text"]
    """

    model_kwargs: Optional[Dict] = None
    """Key word arguments to pass to the model."""

    endpoint_kwargs: Optional[Dict] = None
    """Optional attributes passed to the invoke_endpoint
    function. See `boto3`_. docs for more info.
    .. _boto3: <https://boto3.amazonaws.com/v1/documentation/api/latest/index.html>
    """

    class Config:
        """Configuration for this pydantic object."""

        extra = Extra.forbid
        arbitrary_types_allowed = True

    @root_validator()
    def validate_environment(cls, values: Dict) -> Dict:
        """Validate that AWS credentials to and python package exists in environment."""
        try:
            import boto3

            try:
                if values["credentials_profile_name"] is not None:
                    session = boto3.Session(
                        profile_name=values["credentials_profile_name"]
                    )
                else:
                    # use default credentials
                    session = boto3.Session()

                values["client"] = session.client(
                    "sagemaker-runtime", region_name=values["region_name"]
                )

            except Exception as e:
                raise ValueError(
                    "Could not load credentials to authenticate with AWS client. "
                    "Please check that credentials in the specified "
                    "profile name are valid."
                ) from e

        except ImportError:
            raise ValueError(
                "Could not import boto3 python package. "
                "Please it install it with `pip install boto3`."
            )
        return values

    def _embedding_func(self, texts: List[str]) -> List[float]:
        """Call out to SageMaker Inference embedding endpoint."""
        # replace newlines, which can negatively affect performance.
        _model_kwargs = self.model_kwargs or {}
        _endpoint_kwargs = self.endpoint_kwargs or {}

        body = self.content_handler.transform_input(texts, _model_kwargs)
        content_type = self.content_handler.content_type
        accepts = self.content_handler.accepts

        # send request
        try:
            response = self.client.invoke_endpoint(
                EndpointName=self.endpoint_name,
                Body=body,
                ContentType=content_type,
                Accept=accepts,
                **_endpoint_kwargs,
            )
        except Exception as e:
            raise ValueError(f"Error raised by inference endpoint: {e}")

        return self.content_handler.transform_output(response["Body"])

    def rerank_documents(
            self, query: str, documents: List[Document]
    ) -> Tuple[List[Document], List[float]]:
        """Compute doc embeddings using a SageMaker Inference Endpoint.

        Args:
            query: The query of user.
            documents: documents from Opensearch

        Returns:
            List of reranked documents
        """

        if len(documents) == 0:
            return zip([], [])

        input_pairs = [(query, d.page_content) for d in documents]
        score = self._embedding_func(input_pairs)
        print(type(score))

        if type(score) == float:
            score = [score]

        assert len(score) == len(input_pairs)
        sorted_data = sorted(zip(documents, score), key=lambda x: x[1], reverse=True)
        return sorted_data
