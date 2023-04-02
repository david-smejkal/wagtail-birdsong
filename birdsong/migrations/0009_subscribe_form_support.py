# Generated by Django 4.1.5 on 2023-03-12 14:38

import birdsong.models
from django.db import migrations, models

class Migration(migrations.Migration):

    dependencies = [
        ('birdsong', '0008_translation_support'),
    ]

    operations = [
        migrations.AddField(
            model_name='contact',
            name='created_at',
            field=models.DateTimeField(auto_now_add=True, null=True),
        ),
        migrations.AddField(
            model_name='contact',
            name='updated_at',
            field=models.DateTimeField(auto_now=True, null=True),
        ),
        migrations.AlterField(
            model_name='contact',
            name='email',
            field=models.EmailField(max_length=254, unique=True, verbose_name='email'),
        ),
        migrations.AddField(
            model_name='contact',
            name='is_active',
            field=models.BooleanField(default=birdsong.models.Contact.get_default_is_active),
        ),
    ]