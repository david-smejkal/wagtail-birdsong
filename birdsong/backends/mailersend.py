"""
Email backend that uses the Mailersend API to test and send Campaigns.
"""
import logging
from smtplib import SMTPException
from threading import Thread

from django.db import close_old_connections, transaction
from django.template.loader import render_to_string
from django.utils import timezone

from birdsong.models import Campaign, CampaignStatus, Contact

from django.conf import settings
from birdsong.conf import MAX_NO_OF_BULK_EMAIL_STATUS_CHECKS

from django.core.mail import EmailMessage
# from django.core.mail.backends.base import BaseEmailBackend
from . import BaseEmailBackend
from django.core.mail.message import sanitize_address
from django.contrib import messages
from django.utils.translation import gettext as _

from mailersend import emails
from requests import RequestException
import json
import time
import re

logger = logging.getLogger(__name__)

ERROR_CODE_DAILY_API_QUOTA_LIMIT_REACHED = 429
ERROR_MESSAGE_DAILY_API_QUOTA_LIMIT_REACHED = "MailerSend Error: Daily API quota limit was reached."

class MailerSendException(Exception):
    def __init__(self, message, code):            
        # Call the base class constructor with the parameters it needs
        super().__init__(message)
        self.code = code

class SendCampaignThread(Thread):   
    def __init__(self, request, campaign, contacts, messages, backend):
        super().__init__()
        self.request = request
        self.campaign = campaign
        self.contacts = contacts
        self.messages = messages
        self.backend = backend

    def run(self):
        try:
            self.campaign.status = CampaignStatus.UNSENT
            self.campaign.save() # status to revert back to if things go south
            logger.info(f"Sending {len(self.messages)} emails")
            with transaction.atomic():
                self.campaign.status = CampaignStatus.SENDING
                self.campaign.save()
                (code, response) = self.backend.send_bulk(self.messages)
                logger.info("Campaign information passed over to MailerSend")
                logger.info(f"Parsed code: {code}, response: {response}")
                self.check_status(code, response)
            
        except MailerSendException as e:
            logger.exception(f"Problem sending campaign: {self.campaign.id} due to a MailerSend error.")
            # messages.error(self.request, "Hello") # TODO: figure out why the messages aren't working anymore / intermittently in this Thread
            if e.code == ERROR_CODE_DAILY_API_QUOTA_LIMIT_REACHED:
                messages.error(self.request, _(f"{ERROR_MESSAGE_DAILY_API_QUOTA_LIMIT_REACHED}"))
            else:
                messages.error(self.request, _(f"Unable to send campaign due to a MailerSend error"))
            # timer.sleep(1)
            # self.campaign.status = CampaignStatus.UNSENT # TODO: Should the campaign be set to FAILED instead?
            # self.campaign.save()
        except:
            logger.exception(f"Problem sending campaign: {self.campaign.id} due to a generic error.")
            messages.error(self.request, _(f"Unable to send campaign due to an error"))
            # timer.sleep(1)
            # self.campaign.status = CampaignStatus.UNSENT # TODO: Should the campaign be set to FAILED instead?
            # self.campaign.save()
        finally:
            close_old_connections()
    
    def check_status(self, code, response):
        """
        Checks ... TODO: elaborate
        :param mailer_send_bulk_response - (int, {}) - e.g.
            (202, {"message":"The bulk email is being processed.","bulk_email_id":"63dc744e837a822014066875"})

        QUEUED dictionary example:
        {'data': {
            'id': '63e9130bdcbf5643050513cb', 'state': 'queued', 'total_recipients_count': 1,
            'suppressed_recipients_count': 0, 'suppressed_recipients': None,
            'validation_errors_count': 0, 'validation_errors': None,
            'messages_id': None,
            'created_at': '2023-02-12T16:25:47.924000Z', 'updated_at': '2023-02-12T16:25:47.924000Z'
        }}

        COMPLETED dictionary example:
        {'data': {
            'id': '63e9130bdcbf5643050513cb', 'state': 'completed', 'total_recipients_count': 1,
            'suppressed_recipients_count': 0, 'suppressed_recipients': None,
            'validation_errors_count': 0, 'validation_errors': None,
            'messages_id': ['63e9130dc20a729ad4083df2'],
            'created_at': '2023-02-12T16:25:47.924000Z', 'updated_at': '2023-02-12T16:25:49.183000Z'
        }}
        """
        try:
            # (code, response) = mailer_send_bulk_response
            logger.info(f"Checking code: {code}, response: {response}")
            bulk_email_id = response['bulk_email_id']
            logger.info(f"Checking status of campaign: {self.campaign.id}, bulk_email_id: {bulk_email_id}")
            still_processing = True
            checked_n_amount_of_times = 0

            while still_processing and checked_n_amount_of_times < MAX_NO_OF_BULK_EMAIL_STATUS_CHECKS:
                (code, response) = self.backend.get_bulk_status_by_id(bulk_email_id)
                logger.info(f"Checking bulk status Code: {code}, and Response: {response}")
                # raise Exception()

                if "data" in response and "state" in response['data'] and response['data']['state'] == "completed":
                    still_processing = False
                    self.mark_completed(response['data']['validation_errors']) # TODO: Move this call out of this method

                if not code and "message" in response and response['message'] == "Resource not found.":
                    logger.exception(f"Unable to check back on the bulk email with bulk_email_id: {bulk_email_id}")
                    # raise Exception()
                
                checked_n_amount_of_times += 1
                time.sleep(5) # sleep for 3 seconds

            self.campaign.status = CampaignStatus.UNSENT # tmp
            self.campaign.save()

        except:
            logger.exception(f"Problem with checking status of campaign: {self.campaign.id}")
            # self.campaign.status = CampaignStatus.FAILED # TODO: enable this one
            # self.campaign.status = CampaignStatus.SENT # tmp
            # self.campaign.save()
        finally:
            close_old_connections()


    def mark_completed(self, validation_errors):
        """
        TODO: elaborate
        """
        unsuccessful_contacts = []
        for key, validation_error in validation_errors.items():
            try:
                contact_index = int(key.split('.')[1])
                contact = self.contacts[contact_index]
                logger.info(f"INFO: Couldn't send campaign to contact with email: {contact.email} due to validation error: {validation_error}")
                # self.contacts[contact_index].success = 0
                unsuccessful_contacts.add(self.contacts[contact_index].id)
                # self.contacts.exclude(id=contact_index)
            except:
                logger.exception(f"Problem with parsing of a validation error with key: {key}, value: {validation_error}")

        with transaction.atomic():
            for contact in self.contacts:
                Receipt(campaign_id=self.campaign.id, contact_id=contact.id, success=(0 if contact.id in unsuccessful_contacts else 1)).save()
            # self.campaign.receipts.add(*self.contacts)
            self.campaign.status = CampaignStatus.SENT
            self.campaign.save()

class MailersendEmailBackend(BaseEmailBackend):

    def __init__(self):
        super().__init__()
        # self.mailer = emails.NewEmail(settings.BIRDSONG_MAILERSEND_API_KEY) # TODO: uncomment

    def parse_response(self, response):
        """
        TODO: Elaborate
        """
        logger.info(f"Parsing MailerSend Response: '{response}' Type: {type(response)}")
        if not response:
            return None
        try: # raw json message
            return (None, json.loads(response))
        except:
            pass # Not a simple json response let's try something else
        try: # error code followed by a json message
            response_split = re.findall(r'^(\d+)\s+(.*)$', response, flags=re.DOTALL)[0] # DOTALL makes dots to match linebreaks
            logger.info(response_split)
            logger.info(response_split[0])
            logger.info(response_split[1])
            (code, message) = (int(response_split[0]), json.loads(response_split[1]))
        except Exception as e:
            logger.exception(f"Problem with parsing of MailerSend's Response: {response}")
            raise e
        logger.info(f"Parsed to Code: {code}, Message: {message}")
        if code == ERROR_CODE_DAILY_API_QUOTA_LIMIT_REACHED:
            # logger.exception(message['message'])
            # raise Exception(message['message'])
            raise MailerSendException(message['message'], code) # Let's stop here, no point in proceeding further when out of API calls
        return (code, message)

    def get_bulk_status_by_id(self, bulk_email_id):
        """
        {
            'data': {
                'id': '63e9130bdcbf5643050513cb',
                'state': 'queued',
                'total_recipients_count': 1,
                'suppressed_recipients_count': 0,
                'suppressed_recipients': None,
                'validation_errors_count': 0,
                'validation_errors': None,
                'messages_id': None,
                'created_at': '2023-02-12T16:25:47.924000Z',
                'updated_at': '2023-02-12T16:25:47.924000Z'
            }
        }
        """

        # return self.parse_response(self.mailer.get_bulk_status_by_id(bulk_email_id)) # TODO: uncomment
        return (None, {'data': {'id': '63e967350ee94e23c308236a', 'state': 'completed', 'total_recipients_count': 4, 'suppressed_recipients_count': 1, 'suppressed_recipients': {'63e967365ec84f8724074ce3': {'to': [{'email': 'underlivaerable@raquel.yoga', 'name': None, 'reasons': ['on_hold']}]}}, 'validation_errors_count': 2, 'validation_errors': {'message.1': {'to.0.email': ['Recipient domain must match senders domain.']}, 'message.3': {'to.0.email': ['Recipient domain must match senders domain.']}}, 'messages_id': ['63e967365ec84f8724074ce2', '63e967365ec84f8724074ce3'], 'created_at': '2023-02-12T22:24:53.707000Z', 'updated_at': '2023-02-12T22:24:54.384000Z'}})
        text = '{"message":"Resource not found."}'
        return self.parse_response(text)

    def send_bulk(self, messages):
        """
        TODO: rework this comment, it's out of date
        Asynchronously sends out emails defined by email_list via MailerSend API.
        Returned bulk_email_id can be afterwards passed to get_bulk_status_by_id() to check on the progress.

        :param email_list: List of email dictionaries - @see https://github.com/mailersend/mailersend-python#send-bulk-email
        :return: {"message":"The bulk email is being processed.","bulk_email_id":"63dc744e837a822014066875"}
        """
        # return self.parse_response(self.mailer.send_bulk(self.get_email_list(messages))) # TODO: uncomment
        return self.parse_response('429\n{\n\t"message": "Daily API quota limit was reached."\n}')
        # text = "{\"message\":\"The bulk email is being processed.\",\"bulk_email_id\":\"63dc744e837a822014066875\"}"
        # text = "{\"message\":\"The bulk email is being processed.\",\"bulk_email_id\":\"63e9130bdcbf5643050513cb\"}"
        # return self.parse_response(f"202\n{text}")

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
            (code, response) = self.send_bulk(messages)
            logger.info(f"Parsed test email code: {code}, response: {response}")
        else:
            campaign_thread = SendCampaignThread(request, campaign, contacts, messages, self)
            campaign_thread.start()
