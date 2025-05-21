from typing import Literal

from src.dtos import BaseDTO, GenerateTypeAdapter
from src.processors.conn_configs import IBaseConnConfig


class __IConfig__(BaseDTO):
    projectId: str
    privateKeyId: str
    privateKey: str
    clientEmail: str
    clientId: str
    authUri: str
    tokenUri: str
    authProviderX509CertUrl: str
    clientX509CertUrl: str
    universeDomain: str


class IGcpStorageConnConfig(IBaseConnConfig):
    type: Literal["gcpStorage"]
    config: __IConfig__


GcpStorageConnConfigDTO = GenerateTypeAdapter[IGcpStorageConnConfig](IGcpStorageConnConfig)
