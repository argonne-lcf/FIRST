from django.db import migrations, models


def populate_denormalized_fields(apps, schema_editor):
    BatchLog = apps.get_model("resource_server_async", "BatchLog")
    for batch in BatchLog.objects.select_related("access_log__user").iterator():
        batch._access_log_id_new = str(batch.access_log_id)
        batch.user_id = batch.access_log.user_id or ""
        batch.save(update_fields=["_access_log_id_new", "user_id"])


class Migration(migrations.Migration):
    dependencies = [
        ("resource_server_async", "0012_endpoint_tpm_model_endpoint_tpm_user"),
    ]

    operations = [
        # 1. Add temporary CharField to hold the access_log id as a string
        migrations.AddField(
            model_name="batchlog",
            name="_access_log_id_new",
            field=models.CharField(default="", max_length=100, editable=False),
            preserve_default=False,
        ),
        # 2. Add the new user_id CharField
        migrations.AddField(
            model_name="batchlog",
            name="user_id",
            field=models.CharField(default="", max_length=100),
            preserve_default=False,
        ),
        # 3. Populate both from the existing relationship
        migrations.RunPython(
            populate_denormalized_fields,
            migrations.RunPython.noop,
        ),
        # 4. Drop the OneToOneField (removes the old access_log_id column + FK constraint)
        migrations.RemoveField(
            model_name="batchlog",
            name="access_log",
        ),
        # 5. Rename the temp column to access_log_id
        migrations.RenameField(
            model_name="batchlog",
            old_name="_access_log_id_new",
            new_name="access_log_id",
        ),
    ]
