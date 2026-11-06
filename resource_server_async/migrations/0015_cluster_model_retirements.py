from django.db import migrations, models


class Migration(migrations.Migration):
    dependencies = [("resource_server_async", "0014_remove_accesslog_user_remove_requestlog_access_log_and_more")]

    operations = [
        migrations.AddField(
            model_name="cluster",
            name="model_retirements",
            field=models.JSONField(blank=True, default=list, editable=False),
        ),
    ]
