from django.db import models

class ImportedFiles(models.Model):
    file_name = models.CharField(max_length=200)
    created_at = models.DateTimeField(auto_now_add=True)
