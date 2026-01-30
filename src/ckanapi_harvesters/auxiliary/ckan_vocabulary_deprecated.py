#!python3
# -*- coding: utf-8 -*-
"""
CKAN tag vocabulary information
"""
from typing import List, Dict, Union
import copy

from ckanapi_harvesters.auxiliary.ckan_model import CkanTagInfo
from ckanapi_harvesters.auxiliary.ckan_map import CkanMapABC
from ckanapi_harvesters.auxiliary.ckan_errors import NotMappedObjectNameError

class CkanTagVocabularyInfo:
    def __init__(self, d: dict):
        self.vocabulary_name: str = d["name"]
        self.tags: Dict[str, CkanTagInfo] = {tag_dict["name"]: CkanTagInfo.from_dict(tag_dict) for tag_dict in d["tags"]}
        self.id: str = d["id"]
        self.details:dict = d

    def __str__(self):
        return f"Vocabulary '{self.vocabulary_name}' ({self.id})"

    def to_dict(self, include_details:bool=True) -> dict:
        d = dict()
        if self.details is not None and include_details:
            d.update(self.details)
        d.update({"id": self.id, "name": self.vocabulary_name,
                  "tags": [tag_info.to_dict() for tag_info in self.tags.values()]})
        return d

    @staticmethod
    def from_dict(d:dict) -> "CkanTagVocabularyInfo":
        return CkanTagVocabularyInfo(d)


class CkanVocabularyMap(CkanMapABC):
    def __init__(self):
        self.vocabularies: Dict[str, CkanTagVocabularyInfo] = {}  # id -> info
        self.vocabulary_id_index: Dict[str, str] = {}  # name -> id
        self.vocabularies_listed: bool = False
        self._mapping_query_vocabulary_list: bool = True

    def purge(self):
        self.vocabularies = None
        self.vocabulary_id_index = None
        self.vocabularies_listed = False

    def copy(self) -> "CkanVocabularyMap":
        return copy.deepcopy(self)

    def to_dict(self) -> dict:
        return {"vocabularies":[vocabulary_info.to_dict() for vocabulary_info in self.vocabularies.values()],
                }

    def update_from_dict(self, data:dict) -> None:
        for vocabulary_dict in data["packages"]:
            self._update_vocabulary_info(CkanTagVocabularyInfo.from_dict(vocabulary_dict))

    @staticmethod
    def from_dict(d: dict) -> "CkanVocabularyMap":
        map = CkanVocabularyMap()
        map.update_from_dict(d)
        return map

    ## Vocabulary functions
    def get_vocabulary_id(self, vocabulary_name:str, *, error_not_mapped:bool=True, search_title:bool=True) -> Union[str,None]:
        """
        Retrieve the vocabulary id for a given vocabulary name based on the vocabulary map.

        :param vocabulary_name: vocabulary name or id.
        :return:
        """
        if vocabulary_name is None:
            raise ValueError("vocabulary_name cannot be None")
        if vocabulary_name in self.vocabularies.keys():
            # recognized vocabulary_id
            vocabulary_id = vocabulary_name
        elif vocabulary_name in self.vocabulary_id_index.keys():
            vocabulary_id = self.vocabulary_id_index[vocabulary_name]
        elif error_not_mapped:
            raise NotMappedObjectNameError(f"Vocabulary {vocabulary_name} is not mapped or does not exist.")
        else:
            vocabulary_id = None
        return vocabulary_id

    def _update_vocabulary_info(self, vocabulary_info:Union[CkanTagVocabularyInfo, List[CkanTagVocabularyInfo]],
                                vocabularies_listed:bool=False) -> None:
        """
        Internal function to update the information of a vocabulary.
        """
        if not(isinstance(vocabulary_info, list)):
            vocabulary_info = [vocabulary_info]
        self.vocabularies.update({vocab_info.id: vocab_info for vocab_info in vocabulary_info})
        self.vocabulary_id_index.update({vocab_info.vocabulary_name: vocab_info.id for vocab_info in vocabulary_info})
        if vocabularies_listed:
            self.vocabularies_listed = True

    def _record_vocabulary_delete(self, vocabulary_id: str) -> None:
        # only pass in delete state
        vocabulary_info = self.vocabularies[vocabulary_id]
        self.vocabulary_id_index.pop(vocabulary_info.vocabulary_name)
        self.vocabularies.pop(vocabulary_id)


