import grpc
import user_profile_pb2
import user_profile_pb2_grpc


def run():
    with grpc.insecure_channel("localhost:50051") as channel:
        stub = user_profile_pb2_grpc.UserProfileServiceStub(channel)

        response = stub.CreateUser(
            user_profile_pb2.UserProfileRequest(
                user_id="1", name="Alice", email="alice@example.com", age=25
            )
        )
        print(response.message)

        response = stub.GetUser(user_profile_pb2.UserIdentifier(user_id="1"))
        print(response.message)
        print(response.user_profile)

        response = stub.UpdateUser(
            user_profile_pb2.UserProfileRequest(
                user_id="1", name="Alice", email="alice@example.com", age=20
            )
        )
        print(response.message)
        print(response.user_profile)


if __name__ == "__main__":
    run()
