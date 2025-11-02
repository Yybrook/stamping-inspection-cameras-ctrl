from tortoise import fields, models

class ShuttleImage(models.Model):
    id = fields.IntField(pk=True)
    time = fields.DatetimeField(auto_now_add=True)
    part_id = fields.IntField()
    part_count = fields.IntField()
    camera_ip = fields.CharField(max_length=20)
    camera_user_id = fields.CharField(max_length=20)
    frame_num = fields.IntField()
    frame_t = fields.BigIntField()
    frame_width = fields.IntField()
    frame_height = fields.IntField()
    frame_size = fields.IntField()
    shuttle_has_part_t = fields.BigIntField()
    image_path = fields.CharField(max_length=255)
    res_match = fields.BooleanField(null=True)
    res_prediction = fields.BooleanField(null=True)

    class Meta:
        table = "shuttle_image"
