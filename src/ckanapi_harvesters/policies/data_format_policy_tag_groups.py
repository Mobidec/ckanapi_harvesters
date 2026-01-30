#!python3
# -*- coding: utf-8 -*-
"""
Data format policy representation and enforcing for lists of tags grouped in vocabularies
"""
from typing import List, Dict

from ckanapi_harvesters.auxiliary.error_level_message import ErrorLevel
from ckanapi_harvesters.auxiliary.ckan_auxiliary import assert_or_raise
from ckanapi_harvesters.auxiliary.ckan_errors import MandatoryAttributeError
from ckanapi_harvesters.policies.data_format_policy_lists import ValueListPolicy, GroupedValueListPolicy, ExtraValueListPolicy
from ckanapi_harvesters.policies.data_format_policy_defs import ListChoiceMode
from ckanapi_harvesters.policies.data_format_policy_defs import StringValueSpecification

tag_subs_re = r"[^a-zA-Z0-9_\-\.]"


class TagListPolicy(ValueListPolicy):
    def get_tags_list_dict(self, vocabulary_id: str=None) -> List[Dict[str, str]]:
        """
        Generate tags dictionary to initiate a vocabulary using the CKAN API.
        :param vocabulary_id:
        :return:
        """
        if vocabulary_id is not None:
            tags_list_dict = [{"name": tag_spec.value, "vocabulary_id": vocabulary_id} for tag_spec in self.list_specs]
        else:
            tags_list_dict = [{"name": tag_spec.value} for tag_spec in self.list_specs]
        return tags_list_dict


class TagGroupsListPolicy(GroupedValueListPolicy):
    pass


