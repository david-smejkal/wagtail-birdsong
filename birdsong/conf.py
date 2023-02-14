from django.conf import settings

BIRDSONG_TEST_CONTACT = getattr(settings, 'BIRDSONG_TEST_CONTACT', { 'email': 'wagtail.birdsong@example.com' })
MAILERSEND_MAX_NO_OF_BULK_STATUS_CHECKS = getattr(settings, 'MAILERSEND_MAX_NO_OF_BULK_STATUS_CHECKS', 10)
MAILERSEND_BULK_STATUS_CHECK_TIMEOUT = getattr(settings, 'MAILERSEND_BULK_STATUS_CHECK_TIMEOUT', 5) # in seconds