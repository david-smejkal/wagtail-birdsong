"""
Email backend that uses the Mailersend API.
"""
import logging
from smtplib import SMTPException
from threading import Thread

from django.db import close_old_connections, transaction
from django.template.loader import render_to_string
from django.utils import timezone

from birdsong.models import Campaign, CampaignStatus, Contact
from birdsong.utils import send_mass_html_mail

from django.conf import settings
from django.core.mail import EmailMessage
# from django.core.mail.backends.base import BaseEmailBackend
from . import BaseEmailBackend
from django.core.mail.message import sanitize_address

from mailersend import emails
from requests import RequestException

logger = logging.getLogger(__name__)


class SendCampaignThread(Thread):
    def __init__(self, campaign_pk, contact_pks, messages, backend):
        super().__init__()
        self.campaign_pk = campaign_pk
        self.contact_pks = contact_pks
        self.messages = messages
        self.backend = backend

    def run(self):
        try:
            logger.info(f"Sending {len(self.messages)} emails")
            # self.backend.send_messages(self.messages)
            self.backend.send_bulk(self.backend.get_email_list(self.messages))
            logger.info("Emails finished sending")
            with transaction.atomic():
                Campaign.objects.filter(pk=self.campaign_pk).update(
                    status=CampaignStatus.SENT,
                    sent_date=timezone.now(),
                )
                fresh_contacts = Contact.objects.filter(
                    pk__in=self.contact_pks)
                Campaign.objects.get(
                    pk=self.campaign_pk).receipts.add(*fresh_contacts)
        except SMTPException:
            logger.exception(f"Problem sending campaign: {self.campaign_pk}")
            self.campaign.status = CampaignStatus.FAILED
        finally:
            close_old_connections()


class MailersendEmailBackend(BaseEmailBackend):
    def send_message(self, email_message: EmailMessage):
        if not email_message.recipients():
            return False

        encoding = email_message.encoding or settings.DEFAULT_CHARSET

        mailer = emails.NewEmail(settings.BIRDSONG_MAILERSEND_API_KEY)

        mail_body = {}
        mailer.set_mail_from(
            dict(email=sanitize_address(email_message.from_email, encoding)), mail_body
        )
        mailer.set_mail_to(
            [
                dict(email=sanitize_address(addr, encoding))
                for addr in email_message.to
            ],
            mail_body,
        )
        mailer.set_cc_recipients(
            [
                dict(email=sanitize_address(addr, encoding))
                for addr in email_message.cc
            ],
            mail_body,
        )
        mailer.set_bcc_recipients(
            [
                dict(email=sanitize_address(addr, encoding))
                for addr in email_message.bcc
            ],
            mail_body,
        )
        mailer.set_subject(email_message.subject, mail_body)
        # mailer.set_plaintext_content(email_message.body, mail_body) # TODO: generate plaintext content
        mailer.set_html_content(email_message.body, mail_body)

        try:
            mailer.send(mail_body)
        except RequestException:
            if not self.fail_silently:
                raise
            return False
        return True

    def send_messages(self, email_messages):
        if not email_messages:
            return 0

        num_sent = 0
        for message in email_messages:
            if self.send_message(message):
                num_sent += 1

        return num_sent

    def send_bulk(self, email_list):
        """
        Asynchronously sends out emails defined by email_list via MailerSend API.
        Returned bulk_email_id can be afterwards passed to get_bulk_status_by_id() to check on the progress.

        :param email_list: List of email dictionaries - @see https://github.com/mailersend/mailersend-python#send-bulk-email
        :return: {"message":"The bulk email is being processed.","bulk_email_id":"63dc744e837a822014066875"}
        """
        mailer = emails.NewEmail(settings.BIRDSONG_MAILERSEND_API_KEY)
        return mailer.send_bulk(email_list)


    def get_email_list(self, email_messages):
        mail_list = []
        for email_message in email_messages:
            encoding = email_message.encoding or settings.DEFAULT_CHARSET
            mail_list.append(
                {
                    "from": {
                        "email": sanitize_address(email_message.from_email, encoding)
                    },
                    "to": [dict(email=sanitize_address(addr, encoding)) for addr in email_message.to],
                    "subject": email_message.subject,
                    "text": "", # TODO: generate plaintext content
                    "html": email_message.body
                }
            )
        return mail_list


    def send_campaign(self, request, campaign, contacts, test_send=False):
        messages = []

        for contact in contacts:
            content = render_to_string(
                campaign.get_template(request),
                campaign.get_context(request, contact),
            )
            messages.append(EmailMessage(
                subject=campaign.subject,
                body=content,
                from_email=self.from_email,
                to=[contact.email],
                reply_to=[self.reply_to],
            ))
        if test_send:
            # Don't mark as complete, don't worry about threading
            # self.send_messages(messages)
            self.send_bulk(self.get_email_list(messages))
        else:
            campaign_thread = SendCampaignThread(
                campaign.pk, [c.pk for c in contacts], messages, self)
            campaign_thread.start()
