from django.conf import settings

BIRDSONG_TEST_CONTACT = getattr(settings, 'BIRDSONG_TEST_CONTACT', { 'email': 'wagtail.birdsong@example.com' })
MAX_NO_OF_BULK_EMAIL_STATUS_CHECKS = getattr(settings, 'MAX_NO_OF_BULK_EMAIL_STATUS_CHECKS', 5)