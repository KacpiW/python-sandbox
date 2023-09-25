from concurrent import futures
from email import message
import grpc
from grpc import StatusCode, RpcError
import user_profile_pb2
import user_profile_pb2_grpc


class UserProfileServiceServicer(user_profile_pb2_grpc.UserProfileServiceServicer):
    def __init__(self):
        self.user_profiles = {}

    def CreateUser(self, request, context):
        self.user_profiles[request.user_id] = request
        return user_profile_pb2.UserProfileResponse(
            message="User created successfully", user_profile=request
        )

    def GetUser(self, request, context):
        user = self.user_profiles.get(request.user_id)
        if user:
            return user_profile_pb2.UserProfileResponse(
                message="User retrieved successfully", user_profile=user
            )
        else:
            context.set_code(StatusCode.NOT_FOUND)
            context.set_details("User not found")
            raise RpcError("User cannot be found")

    def UpdateUser(self, request, context):
        user = self.user_profiles.get(request.user_id)
        if user:
            self.user_profiles[request.user_id] = request
            return user_profile_pb2.UserProfileResponse(
                message="User successfully modified", user_profile=request
            )
        else:
            context.set_code(StatusCode.NOT_FOUND)
            context.set_details("User does not exists in db")
            raise RpcError("User does not exists in db")

    def DeleteUser(self, request, context):
        pass

    def ListUsers(self, request, context):
        pass

    # Implement other RPC methods (GetUser, UpdateUser, DeleteUser, ListUsers) similarly


def serve():
    server = grpc.server(futures.ThreadPoolExecutor(max_workers=10))
    user_profile_pb2_grpc.add_UserProfileServiceServicer_to_server(
        UserProfileServiceServicer(), server
    )
    server.add_insecure_port("[::]:50051")
    server.start()
    server.wait_for_termination()


if __name__ == "__main__":
    serve()
