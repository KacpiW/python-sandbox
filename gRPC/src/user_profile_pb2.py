# -*- coding: utf-8 -*-
# Generated by the protocol buffer compiler.  DO NOT EDIT!
# source: user_profile.proto
"""Generated protocol buffer code."""
from google.protobuf import descriptor as _descriptor
from google.protobuf import descriptor_pool as _descriptor_pool
from google.protobuf import symbol_database as _symbol_database
from google.protobuf.internal import builder as _builder

# @@protoc_insertion_point(imports)

_sym_db = _symbol_database.Default()


DESCRIPTOR = _descriptor_pool.Default().AddSerializedFile(
    b'\n\x12user_profile.proto\x12\x0buserprofile"O\n\x12UserProfileRequest\x12\x0f\n\x07user_id\x18\x01 \x01(\t\x12\x0c\n\x04name\x18\x02 \x01(\t\x12\r\n\x05\x65mail\x18\x03 \x01(\t\x12\x0b\n\x03\x61ge\x18\x04 \x01(\x05"!\n\x0eUserIdentifier\x12\x0f\n\x07user_id\x18\x01 \x01(\t"]\n\x13UserProfileResponse\x12\x0f\n\x07message\x18\x01 \x01(\t\x12\x35\n\x0cuser_profile\x18\x02 \x01(\x0b\x32\x1f.userprofile.UserProfileRequest"\x12\n\x10ListUsersRequest"K\n\x11ListUsersResponse\x12\x36\n\ruser_profiles\x18\x01 \x03(\x0b\x32\x1f.userprofile.UserProfileRequest2\x99\x03\n\x12UserProfileService\x12O\n\nCreateUser\x12\x1f.userprofile.UserProfileRequest\x1a .userprofile.UserProfileResponse\x12H\n\x07GetUser\x12\x1b.userprofile.UserIdentifier\x1a .userprofile.UserProfileResponse\x12O\n\nUpdateUser\x12\x1f.userprofile.UserProfileRequest\x1a .userprofile.UserProfileResponse\x12K\n\nDeleteUser\x12\x1b.userprofile.UserIdentifier\x1a .userprofile.UserProfileResponse\x12J\n\tListUsers\x12\x1d.userprofile.ListUsersRequest\x1a\x1e.userprofile.ListUsersResponseb\x06proto3'
)

_globals = globals()
_builder.BuildMessageAndEnumDescriptors(DESCRIPTOR, _globals)
_builder.BuildTopDescriptorsAndMessages(DESCRIPTOR, "user_profile_pb2", _globals)
if _descriptor._USE_C_DESCRIPTORS == False:
    DESCRIPTOR._options = None
    _globals["_USERPROFILEREQUEST"]._serialized_start = 35
    _globals["_USERPROFILEREQUEST"]._serialized_end = 114
    _globals["_USERIDENTIFIER"]._serialized_start = 116
    _globals["_USERIDENTIFIER"]._serialized_end = 149
    _globals["_USERPROFILERESPONSE"]._serialized_start = 151
    _globals["_USERPROFILERESPONSE"]._serialized_end = 244
    _globals["_LISTUSERSREQUEST"]._serialized_start = 246
    _globals["_LISTUSERSREQUEST"]._serialized_end = 264
    _globals["_LISTUSERSRESPONSE"]._serialized_start = 266
    _globals["_LISTUSERSRESPONSE"]._serialized_end = 341
    _globals["_USERPROFILESERVICE"]._serialized_start = 344
    _globals["_USERPROFILESERVICE"]._serialized_end = 753
# @@protoc_insertion_point(module_scope)
