from tortoise import BaseDBAsyncClient

RUN_IN_TRANSACTION = True


async def upgrade(db: BaseDBAsyncClient) -> str:
    return """
        CREATE TABLE [shuttle_image] (
    [id] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [time] DATETIME2 NOT NULL DEFAULT CURRENT_TIMESTAMP,
    [part_id] INT NOT NULL,
    [part_count] INT NOT NULL,
    [camera_ip] VARCHAR(20) NOT NULL,
    [camera_user_id] VARCHAR(20) NOT NULL,
    [frame_num] INT NOT NULL,
    [frame_t] BIGINT NOT NULL,
    [frame_width] INT NOT NULL,
    [frame_height] INT NOT NULL,
    [frame_size] INT NOT NULL,
    [shuttle_has_part_t] BIGINT NOT NULL,
    [image_path] VARCHAR(255) NOT NULL,
    [res_match] BIT,
    [res_prediction] BIT
);
CREATE TABLE [aerich] (
    [id] INT IDENTITY(1,1) NOT NULL PRIMARY KEY,
    [version] VARCHAR(255) NOT NULL,
    [app] VARCHAR(100) NOT NULL,
    [content] NVARCHAR(MAX) NOT NULL
);"""


async def downgrade(db: BaseDBAsyncClient) -> str:
    return """
        """


MODELS_STATE = (
    "eJztmG1v2jAQx78K4hWTuooGKGzvoGMrU4GppdvUqopMYhKriZ3aziir+t1nO08kJECkqg"
    "PBK8Ld/ZO7X/yUe6m6xIQOO72xfc4dOHCBBaufKy9VDFx5kes/qVSB5yVeaeBg6igBCyJ1"
    "FIdOGafA4MI5Aw6DwmRCZlDkcUSwsGLfcaSRGCIQYSsx+Rg9+VDnxILchlQ47h+EGWETPk"
    "MW/fUe9RmCjpnKG5ny2cqu84WnbAPMv6pA+bSpbhDHd3ES7C24TXAcjTCXVgtiSAGH8vac"
    "+jJ9mV1YblRRkGkSEqS4pDHhDPgOXyp3SwYGwZKfyIapAtX7+aidNdvNTuO82REhKpPY0n"
    "4NyktqD4SKwGhSfVV+wEEQoTAm3Dhy4Sq5LwKA9OTjizQZgGYoOo0usjgjeOt4RoYEaDKI"
    "3ogohcAcY2cRvqw1+CaDYf9m0h3+kJW4jD05Ck530pceTVkXGWvt/IO0EzEFggkS36Tyaz"
    "C5rMi/lbvxqK8IEsYtqp6YxE3uqjIn4HOiYzLXgbk0riJrBEZEJi/TA5TrpWbCkmLzdNiR"
    "9/cmMyIDzSB+UG8ZbrHoUNEZ4pcCHXmr5C5sQPPRpUQZciLZHSXngmfdgdjitsRVX4PpZ/"
    "f64rJ7XdPqmYVgFHo05col6TNIcyfwRpxLyiPTgOmMigsd+26JeZ3SHOq0DiDkLIc9ZG0g"
    "t4/L4SdNazTaWr1x3mk12+1Wpx4DXHWtI9kbfJMwUwO0iO4cmWLYlx2YsWrPEL/x0LQhsu"
    "wy23VWdtj4GPqbc+bfAC8SHSq66DvXBkxXp7+S62O+fs9ovvNSqXoKglbeSll8Ikqr9vQ0"
    "1GptcxxqtYrPQ9KXxik+NHUXcCOHZo8QBwKcDzSly/CcCuEWQMOv13fkuW7wjcdXqS/63i"
    "AzFEe3w17/unam6IogxAtGqETjUWgiQ+VXnmtafIQrG3yzx6VWlTRMgfE4B9TUVzxEI0Wx"
    "qy5Xc7MWgMVaYYZ1yqrC/mcXUmTY1ZzOaOg5WdcTBUnMsRm6c3t6cTP0D6QsdxoXbzZLku"
    "NOE4OUU6MExDB8PwGe1bfpXIioQoDKl+kHEcxhXkfy+814VNAISiQZkLdYFHgvt5mTioMY"
    "f9hNrGsoyqpTm0oErzbs/s5yvbga97LNdXmD3v/eXl7/ATLqu2A="
)
