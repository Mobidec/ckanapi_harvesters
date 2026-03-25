#!python3
# -*- coding: utf-8 -*-
"""
Data format policy representation and enforcing for lists of tags grouped in vocabularies
"""
from typing import List, Dict

from ckanapi_harvesters.policies.data_format_policy_lists import ValueListPolicy, GroupedValueListPolicy

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


