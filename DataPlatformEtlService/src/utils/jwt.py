import jwt


class JWTUtils:
    @staticmethod
    async def decode_token(token: str):
        decoded = jwt.decode(token, verify=False)
        return decoded
