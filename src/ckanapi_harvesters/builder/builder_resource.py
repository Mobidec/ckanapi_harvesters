#!python3
# -*- coding: utf-8 -*-
"""
Code to upload metadata to the CKAN server to create/update an existing package
The metadata is defined by the user in an Excel worksheet
This file implements the basic resources. See builder_datastore for specific functions to initiate datastores.
"""
from typing import Union, Any
from abc import ABC, abstractmethod
import os
from warnings import warn
import copy
import io

import pandas as pd

from ckanapi_harvesters.auxiliary.error_level_message import ContextErrorLevelMessage, ErrorLevel
from ckanapi_harvesters.auxiliary.ckan_auxiliary import upload_prepare_requests_files_arg
from ckanapi_harvesters.auxiliary.ckan_model import CkanResourceInfo
from ckanapi_harvesters.auxiliary.path import resolve_rel_path
from ckanapi_harvesters.ckan_api import CkanApi
from ckanapi_harvesters.auxiliary.ckan_auxiliary import _string_from_element, _bool_from_string
from ckanapi_harvesters.auxiliary.ckan_model import CkanState
from ckanapi_harvesters.auxiliary.ckan_errors import CkanArgumentError, MissingIdError, FunctionMissingArgumentError, MandatoryAttributeError
from ckanapi_harvesters.builder.builder_errors import ResourceFileNotExistMessage, EmptyPackageNameException


builder_request_default_auth_if_ckan:Union[bool,None] = True  # fill authentification headers for requests with CkanApi requests proxy method if same domain is used by default


class BuilderResourceABC(ABC):
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 state:CkanState=None, enable_download:bool=True,
                 resource_id:str=None, download_url:str=None):
        self.name: Union[str,None] = name
        self.format: Union[str,None] = format
        self.description: Union[str,None] = description
        self.state:Union[CkanState,None] = state
        if state is None:
            self.state = CkanState.Active  # activate by default
        self.enable_download:bool = enable_download
        self.options_string: Union[str,None] = None
        # Map information, if present
        self.package_name: str = ""  # parent package name (update before any operation)
        self.known_id: Union[str,None] = resource_id
        self.download_url: Union[str,None] = download_url
        self.comment: Union[str,None] = None
        # Functions inputs/outputs
        self.sample_data_source: str = ""
        self.reupload_on_update: bool = True
        self.downloaded_destination: str = ""
        self.download_skip_existing:bool = True  # True: do not overwrite files
        self.download_error_not_found:bool = True
        self.create_default_view:bool = True

    def __copy__(self):
        return self.copy()

    @abstractmethod
    def copy(self, *, dest=None):
        dest.name = self.name
        dest.format = self.format
        dest.description = self.description
        dest.state = self.state
        dest.enable_download = self.enable_download
        dest.options_string = self.options_string
        dest.package_name = self.package_name
        dest.known_id = self.known_id
        dest.download_url = self.download_url
        dest.comment = self.comment
        dest.sample_data_source = self.sample_data_source
        dest.reupload_on_update = self.reupload_on_update
        dest.downloaded_destination = self.downloaded_destination
        dest.download_skip_existing = self.download_skip_existing
        dest.download_error_not_found = self.download_error_not_found
        dest.create_default_view = self.create_default_view
        return dest

    def _check_mandatory_attributes(self):
        if self.name is None:
            raise MandatoryAttributeError("Resource", "name")

    def init_options_from_ckan(self, ckan:CkanApi) -> None:
        """
        Function to initialize some parameters from the ckan object
        """
        pass

    @abstractmethod
    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        # abstract method because does not take into account file/url field
        self.name = _string_from_element(row["name"]).strip()
        self.format = _string_from_element(row["format"]).upper().strip()
        self.description = None
        if "description" in row.keys():
            self.description = _string_from_element(row["description"])
        self.enable_download = True
        if "options" in row.keys():
            self.options_string = _string_from_element(row["options"], empty_value="")
        if "download" in row.keys():
            self.enable_download = _bool_from_string(row["download"])
        self.state = None
        if "state" in row.keys():
            state = _string_from_element(row["state"])
            if state is not None:
                self.state = CkanState.from_str(state)
        # Map information, if present
        self.known_id = None
        self.download_url = None
        if "known id" in row.keys():
            self.known_id = _string_from_element(row["known id"])
        if "known url" in row.keys():
            self.download_url = _string_from_element(row["known url"])
        if "comment" in row.keys():
            self.comment = _string_from_element(row["comment"])

    def get_or_query_resource_id(self, ckan: CkanApi, cancel_if_present:bool=True, error_not_found:bool=True) -> str:
        """
        Store/retrieve resource ID in the class attributes.
        """
        package_name = self.package_name
        if package_name == "":
            raise EmptyPackageNameException()
        if self.known_id is None or not cancel_if_present:
            ckan.map_resources(package_name, only_missing=True)
            self.known_id = ckan.map.get_resource_id(self.name, package_name=package_name, error_not_mapped=error_not_found)
        return self.known_id

    def get_or_query_package_id(self, ckan: CkanApi) -> str:
        """
        Obtain package ID from the package name. This can lead to a request to the API.
        """
        package_name = self.package_name
        if package_name == "":
            raise EmptyPackageNameException()
        ckan.map_resources(package_name, only_missing=True)
        package_id = ckan.map.get_package_id(package_name=package_name)
        return package_id

    @staticmethod
    @abstractmethod
    def resource_mode_str() -> str:
        raise NotImplementedError()

    def __str__(self):
        return f"Resource builder for {self.name} in mode {self.resource_mode_str()}"

    @abstractmethod
    def _to_dict(self, include_id:bool=True) -> dict:
        d = {
            "Name": self.name,
            "Format": self.format if self.format else "",
            "State": self.state.name if self.state is not None else "",
            "Mode": self.resource_mode_str(),
            "File/URL": None,  # concrete implementations must fill this field
            "Options": self.options_string,
            "Download": str(self.enable_download),
            "Description": self.description if self.description else "",
            "Primary key": "",
            "Indexes": "",
            "Upload function": "",
            "Download function": "",
            "Aliases": "",
            "Comment": self.comment if self.comment else "",
        }
        if include_id and self.known_id is not None:
            d["Known ID"] = self.known_id
        if include_id and self.download_url is not None:
            d["Known URL"] = self.download_url
        return d

    def _to_row(self) -> pd.Series:
        row = pd.Series(self._to_dict())
        row.index = row.index.map(str.lower)
        row.index = row.index.map(str.strip)
        return row

    @staticmethod
    @abstractmethod
    def sample_file_path_is_url() -> bool:
        raise NotImplementedError()

    @abstractmethod
    def get_sample_file_path(self, resources_base_dir:str) -> Union[str,None]:
        """
        Function returning the local resource file name for the sample file.

        :param resources_base_dir: base directory to find the resources on the local machine
        :return:
        """
        raise NotImplementedError()

    @abstractmethod
    def load_sample_data(self, resources_base_dir:str) -> Union[bytes,None]:
        """
        Function returning the data from the indicated resources.

        :param resources_base_dir: base directory to find the resources on the local machine
        :return:
        """
        raise NotImplementedError()

    @abstractmethod
    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        """
        Test the presence of the files/urls used in the upload/patch requests.

        :param resources_base_dir:
        :return: None if success, error message otherwise
        """
        raise NotImplementedError()

    @abstractmethod
    def patch_request(self, ckan:CkanApi, package_id:str, *,
                      reupload:bool=None, resources_base_dir:str=None) -> CkanResourceInfo:
        """
        Function to perform all the necessary requests to initiate/reupload the resource on the CKAN server.

        :param resources_base_dir:
        :param ckan:
        :param reupload: option to reupload the resource
        :return:
        """
        # TODO: call to API resource_patch
        # ckan.resource_patch
        raise NotImplementedError()

    def upload_request(self, resources_base_dir:str, ckan:CkanApi, package_id:str):
        # might be dead code
        # this function (patch_request) gets specialized in certain cases
        return self.patch_request(ckan, package_id, resources_base_dir=resources_base_dir, reupload=True)

    @abstractmethod
    def download_sample(self, ckan:CkanApi, full_download:bool=True, **kwargs) -> bytes:
        """
        Download the resource and return the data as bytes.

        :param ckan:
        :param out_dir:
        :param full_download: Some resources like URLs are not downloaded by default. Large datasets are also limited to one request for this function by default.
        :param threads:
        :return:
        """
        raise NotImplementedError()

    @abstractmethod
    def download_request(self, ckan:CkanApi, out_dir:str, *, full_download:bool=True, force:bool=False, threads:int=1) -> Any:
        """
        Download the resource and save in a file pointed by out_dir.
        In most implementations, this calls the download_sample method.

        :param ckan:
        :param out_dir:
        :param full_download: Some resources like URLs are not downloaded by default. Large datasets are treated with a multi-threaded approach.
        :param threads:
        :param force: option to bypass the enable_download attribute of resources
        :return:
        """
        raise NotImplementedError()

    def _to_ckan_resource_info(self, package_id:str, check_id:bool=True) -> CkanResourceInfo:
        """
        Return resource info object from the information of the Excel workbook.
        No requests are made but to use this data in the ckan object, the ID and name of the resource are mandatory.

        :param package_id:
        :param check_id:
        :return:
        """
        if self.known_id is None and check_id:
            msg = MissingIdError("resource", self.name)
            raise msg
        resource_info = CkanResourceInfo()
        resource_info.id = self.known_id
        resource_info.package_id = package_id
        resource_info.name = self.name
        resource_info.description = self.description
        resource_info.download_url = self.download_url
        return resource_info

    def resource_info_request(self, ckan:CkanApi, error_not_found:bool=True) -> Union[CkanResourceInfo, None]:
        resource_id = self.get_or_query_resource_id(ckan, cancel_if_present=False, error_not_found=error_not_found)
        if resource_id is None and not self.download_error_not_found:
            return None
        res_info = ckan.get_resource_info_or_request(resource_id)
        self.known_id = resource_id
        return res_info

    def delete_request(self, ckan:CkanApi, package_id:str, *, error_not_found:bool=False):
        """
        Delete the resource from the CKAN server.

        :return:
        """
        self.package_name = package_id
        resource_id = self.get_or_query_resource_id(ckan, error_not_found=error_not_found)
        if resource_id is not None:
            ckan.resource_delete(resource_id)


class BuilderFileABC(BuilderResourceABC, ABC):
    """
    Abstract class defining the behavior for a resource represented by a file (not a DataStore)
    """
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, file_name:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        self.file_name: str = file_name

    def copy(self, *, dest=None):
        super().copy(dest=dest)
        dest.file_name = self.file_name
        return dest

    def _check_mandatory_attributes(self):
        super()._check_mandatory_attributes()
        if self.file_name is None:
            raise MandatoryAttributeError(self.resource_mode_str(), "File")

    @abstractmethod
    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row)
        self.file_name = _string_from_element(row["file/url"])

    def patch_request(self, ckan: CkanApi, package_id: str, *, reupload: bool = None, resources_base_dir:str=None,
                      payload:Union[bytes, io.BufferedIOBase]=None) -> CkanResourceInfo:
        """
        Perform a patch of the resource on the CKAN server.
        A patch is a full update of the metadata of the resource, and of the DataStore if appropriate.
        The source file of the resource is also uploaded (or a first file for large DataStores).

        :param ckan:
        :param package_id:
        :param reupload:
        :param resources_base_dir:
        :param payload:
        :return:
        """
        if reupload is None: reupload = self.reupload_on_update
        if payload is None:
            payload = self.load_sample_data(resources_base_dir=resources_base_dir)
        payload_file_name = self.file_name
        files = upload_prepare_requests_files_arg(payload=payload, payload_name=payload_file_name)
        res_info = ckan.resource_create(package_id, name=self.name, format=self.format, description=self.description, state=self.state,
                                        files=files, datastore_create=False, auto_submit=False, create_default_view=self.create_default_view,
                                        cancel_if_exists=True, update_if_exists=True, reupload=reupload)
        self.known_id = res_info.id
        return res_info

    def download_sample(self, ckan:CkanApi, full_download:bool=True, **kwargs) -> Union[bytes, None]:
        resource_id = self.get_or_query_resource_id(ckan=ckan, error_not_found=self.download_error_not_found)
        if resource_id is None and not self.download_error_not_found:
            return None
        resource_info, response = ckan.resource_download(resource_id, **kwargs)
        if response is not None:
            return response.content
        else:
            return None

    def download_request(self, ckan: CkanApi, out_dir: str, *, full_download:bool=True, threads:int=1,
                         force:bool=False, **kwargs) -> None:
        if (not self.enable_download) and (not force):
            msg = f"Did not download resource {self.name} because download was disabled."
            warn(msg)
            return
        if out_dir is not None:
            self.downloaded_destination = resolve_rel_path(out_dir, self.file_name, field=f"File/URL of resource {self.name}")
            if self.download_skip_existing and os.path.exists(self.downloaded_destination):
                return
        content = self.download_sample(ckan=ckan, full_download=full_download, **kwargs)
        if out_dir is not None and content is not None:
            os.makedirs(out_dir, exist_ok=True)
            with open(self.downloaded_destination, "wb") as f:
                f.write(content)
                f.close()


# class BuilderResourceUnmanagedABC(BuilderResourceABC, ABC):
#     # dead code
#     def __init__(self, *, name:str=None, format:str=None, description:str=None,
#                  resource_id:str=None, download_url:str=None):
#         super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
#         self.file_name: str = name
#
#     def copy(self, *, dest=None):
#         super().copy(dest=dest)
#         dest.file_name = self.file_name
#         return dest
#
#     def _load_from_df_row(self, row: pd.Series):
#         super()._load_from_df_row(row=row)
#         self.file_name = self.name
#         self._check_mandatory_attributes()
#
#     def _to_dict(self, include_id:bool=True) -> dict:
#         d = super()._to_dict(include_id=include_id)
#         d["File/URL"] = ""
#         return d
#
#     def load_sample_data(self, resources_base_dir:str) -> bytes:
#         return None


class BuilderResourceUnmanaged(BuilderFileABC):  #, BuilderResourceUnmanagedABC):  # multiple inheritance can give undefined results
    """
    Class to manage a resource metadata without specifying its contents during the upload process.
    """
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        self.file_name: str = name
        self.default_payload: Union[bytes, io.BufferedIOBase, None] = None

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderResourceUnmanaged()
        super().copy(dest=dest)
        dest.file_name = self.file_name
        dest.default_payload = copy.deepcopy(self.default_payload)
        return dest

    @staticmethod
    def resource_mode_str() -> str:
        return "Unmanaged"

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row)
        self.file_name = self.name
        self._check_mandatory_attributes()

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = ""
        return d

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return False

    def get_sample_file_path(self, resources_base_dir:str) -> Union[str,None]:
        return None

    def load_sample_data(self, resources_base_dir:str) -> Union[bytes,None]:
        return None

    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[ContextErrorLevelMessage,None]:
        return None

    def patch_request(self, ckan:CkanApi, package_id:str, *,
                      reupload:bool=None, resources_base_dir:str=None,
                      payload:Union[bytes, io.BufferedIOBase]=None) -> CkanResourceInfo:
        if payload is None:
            payload = self.default_payload
        if reupload is None: reupload = self.reupload_on_update and payload is not None
        payload_file_name = self.file_name
        files = upload_prepare_requests_files_arg(payload=payload, payload_name=payload_file_name) if payload is not None else None
        res_info = ckan.resource_create(package_id, name=self.name, format=self.format, description=self.description, state=self.state,
                                        files=files, datastore_create=False, auto_submit=False, create_default_view=self.create_default_view,
                                        cancel_if_exists=True, update_if_exists=True, reupload=reupload)
        self.known_id = res_info.id
        return res_info


class BuilderFileBinary(BuilderFileABC):
    """
    Concrete implementation for a binary file.
    """
    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderFileBinary()
        super().copy(dest=dest)
        return dest

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return False

    def get_sample_file_path(self, resources_base_dir:str) -> str:
        return resolve_rel_path(resources_base_dir, self.file_name, field=f"File/URL of resource {self.name}")

    def load_sample_data(self, resources_base_dir:str) -> bytes:
        self.sample_source = self.get_sample_file_path(resources_base_dir)
        with open(self.sample_source, "rb") as f:
            contents = f.read()
            f.close()
        return contents

    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        file_path = self.get_sample_file_path(resources_base_dir=resources_base_dir)
        if os.path.isfile(file_path):
            return None
        else:
            return ResourceFileNotExistMessage(self.name, ErrorLevel.Error, f"Missing file for resource {self.name}: {file_path}")

    @staticmethod
    def resource_mode_str() -> str:
        return "File"

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row)
        self._check_mandatory_attributes()

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = self.file_name
        return d


class BuilderUrlABC(BuilderFileABC, ABC):
    """
    Abstract behavior for a resource defined by an external URL.
    """
    def __init__(self, *, name:str=None, format:str=None, description:str=None,
                 resource_id:str=None, download_url:str=None, url:str=None):
        super().__init__(name=name, format=format, description=description, resource_id=resource_id, download_url=download_url)
        self.url = url
        self.file_name: str = name

    def copy(self, *, dest=None):
        super().copy(dest=dest)
        dest.url = self.url
        dest.file_name = self.file_name
        return dest

    def upload_file_checks(self, *, resources_base_dir:str=None, ckan: CkanApi=None, **kwargs) -> Union[None,ContextErrorLevelMessage]:
        if ckan is None:
            return ResourceFileNotExistMessage(self.name, ErrorLevel.Warning, "Could not determine if resource url exists because ckan argument was not provided.")
        else:
            return ckan.download_url_proxy_test_head(self.url, **kwargs)

    def _check_mandatory_attributes(self):
        super()._check_mandatory_attributes()
        if self.url is None:
            raise MandatoryAttributeError(self.resource_mode_str(), "URL")

    def _load_from_df_row(self, row: pd.Series, base_dir:str=None):
        super()._load_from_df_row(row=row)
        self.url: str = _string_from_element(row["file/url"])
        self.file_name = self.name
        self._check_mandatory_attributes()

    def download_request(self, ckan: CkanApi, out_dir: str, *, full_download:bool=False, threads:int=1,
                         force:bool=False, **kwargs) -> None:
        # do not download URLs by default
        if full_download:
            super().download_request(ckan=ckan, out_dir=out_dir,full_download=full_download, force=force,
                                     threads=threads, **kwargs)

    def _to_dict(self, include_id:bool=True) -> dict:
        d = super()._to_dict(include_id=include_id)
        d["File/URL"] = self.url
        return d


class BuilderUrl(BuilderUrlABC):
    """
    Class for a resource defined by an external URL.
    """
    @staticmethod
    def resource_mode_str() -> str:
        return "URL"

    def copy(self, *, dest=None):
        if dest is None:
            dest = BuilderUrl()
        super().copy(dest=dest)
        return dest

    @staticmethod
    def sample_file_path_is_url() -> bool:
        return True

    def get_sample_file_path(self, resources_base_dir: str) -> str:
        return self.url

    def load_sample_data(self, resources_base_dir:str, *, ckan:CkanApi=None,
                         proxies:dict=None, headers:dict=None) -> bytes:
        self.sample_source = self.url
        if ckan is None:
            raise FunctionMissingArgumentError("BuilderDataStoreUrl.load_sample_data", "ckan")
        return ckan.download_url_proxy(self.url, proxies=proxies, headers=headers, auth_if_ckan=builder_request_default_auth_if_ckan).content

    def patch_request(self, ckan: CkanApi, package_id: str, *, reupload: bool = None, resources_base_dir:str=None,
                      payload:Union[bytes, io.BufferedIOBase]=None) -> CkanResourceInfo:
        if reupload is None: reupload = self.reupload_on_update
        if payload is not None:
            raise CkanArgumentError("payload", "resource defined from URL patch")
        return ckan.resource_create(package_id, name=self.name, format=self.format, description=self.description, state=self.state,
                                    url=self.url, auto_submit=False, datastore_create=False, create_default_view=self.create_default_view,
                                    cancel_if_exists=True, update_if_exists=True, reupload=reupload)

