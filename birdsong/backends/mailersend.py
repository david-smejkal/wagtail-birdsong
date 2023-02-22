import re
import json
import logging
from time import sleep
from threading import Thread
from django.conf import settings
from django.db import close_old_connections, transaction
from django.core.mail import EmailMessage
from django.core.mail.message import sanitize_address
from django.contrib import messages
from django.template.loader import render_to_string
from django.utils import timezone
from django.utils.translation import gettext as _
from birdsong.conf import MAILERSEND_MAX_NO_OF_BULK_STATUS_CHECKS, MAILERSEND_BULK_STATUS_CHECK_TIMEOUT
from birdsong.models import CampaignStatus, Receipt
from . import BaseEmailBackend
from mailersend import emails

logger = logging.getLogger(__name__)

ERROR_CODE_UNAUTHORIZED = 403
ERROR_CODE_DAILY_API_QUOTA_LIMIT_REACHED = 429
ERROR_CODE_BULK_EMAIL_NOT_FOUND = "Resource not found." # Unfortunately there's no code for this error so we shall use the message instead
ERROR_CODE_BULK_CHECK_TIMED_OUT = 5001 # Used internally in the backend, not actually returned back by MailerSend
ERROR_CODES = {
    ERROR_CODE_UNAUTHORIZED: CampaignStatus.UNSENT,
    ERROR_CODE_DAILY_API_QUOTA_LIMIT_REACHED: CampaignStatus.UNSENT,
    ERROR_CODE_BULK_EMAIL_NOT_FOUND: CampaignStatus.UNSENT,
    ERROR_CODE_BULK_CHECK_TIMED_OUT: CampaignStatus.SENDING,
}

ERROR_MESSAGE_UNAUTHORIZED = _("This action is unauthorized")
ERROR_MESSAGE_DAILY_API_QUOTA_LIMIT_REACHED = _("Daily API quota limit was reached")
ERROR_MESSAGE_BULK_EMAIL_NOT_FOUND = _("Unable to find bulk email resource")
ERROR_MESSAGE_BULK_CHECK_TIMED_OUT = _("Checking bulk email status timed out") # TODO: Provide ways to re-check automatically or at least manually

BULK_STATE_COMPLETED = "completed"

class MailerSendException(Exception):
    """
    Makes it possible to react to and handle MailerSend specific errors.
    """
    def __init__(self, message, code):            
        super().__init__(message)
        self.code = code
        self.message = message


class SendCampaignThread(Thread):
    """
    Carries out asynchronous "Send Campaign" related operations
    TODO: Use Celery instead of Threading in the future for better scaling and user experience
    """
    def __init__(self, request, campaign, contacts, email_messages, test_send, backend):
        super().__init__()
        self.request = request
        self.campaign = campaign
        self.contacts = contacts
        self.email_messages = email_messages
        self.test_send = test_send
        self.backend = backend

    def run(self):
        """
        Runs a new thread spawned by start().
        """
        try:
            logger.info(f"Sending instructions to send out {len(self.email_messages)} email(s) to MaiilerSend")
            with transaction.atomic():
                (code, response) = self.backend.send_bulk(self.email_messages)
                if self.test_send: # Is this a Send test?
                    # messages.success(self.request, _("Test email sent, please check your inbox"))
                    return # no need to continue any further
                logger.info(f"Campaign information passed over to MailerSend.")
                logger.debug(f"Parsed code: {code}, response: {response}") # TODO: Test debug logs, possibly remove them altogether
                (code, response) = self.check_bulk_status(code, response)
                self.seal_campaign(response['data']['validation_errors'])
            
        except MailerSendException as e:
            logger.exception(f"Problem sending campaign id=\"{self.campaign.id}\" due to a MailerSend error")
            error_message = _("Failed to send campaign") + f" {self.campaign.name} " + _("due to a MailerSend error")
            campaign_fallback_status = CampaignStatus.UNSENT
            if e.code in ERROR_CODES.keys():
                error_message += f": \"{e.message}\""
                campaign_fallback_status = ERROR_CODES.get(e.code, CampaignStatus.UNSENT)
            messages.error(self.request, error_message)
            if self.test_send:
                return
            # NOTE: Unfortunately any raised django messages after an update() or a save() model operation in a threaded process are ignored
            # As such we can't set campaing to SENDING at the start of the transaction to let it then fall back to UNSENT status naturarily
            self.campaign.status = campaign_fallback_status
            self.campaign.save() # fallback campaign status
        except:
            logger.exception(f"Problem sending campaign id=\"{self.campaign.id}\" due to an error")
            error_message = _("Failed to send campaign") + f" {self.campaign.name} " + _("due to an error")
            messages.error(self.request, error_message)
            self.campaign.status = CampaignStatus.UNSENT
            self.campaign.save() # fallback campaign status
        finally:
            close_old_connections()
    
    def check_bulk_status(self, code, response):
        """
        Periodically queries MailerSend to check on the bulk email status of response['bulk_email_id'].

        :param code: int|void - e.g. 202, None
        :param response: dict - e.g. {"message":"The bulk email is being processed.","bulk_email_id":"63e9130dc20a729ad4083df2"}
        :return: (int|void, dict) - e.g.
            (None, {'data': {
                'id': '63e9130bdcbf5643050513cb', 'state': 'completed', 'total_recipients_count': 1,
                'suppressed_recipients_count': 0, 'suppressed_recipients': None,
                'validation_errors_count': 0, 'validation_errors': None,
                'messages_id': ['63e9130dc20a729ad4083df2'],
                'created_at': '2023-02-12T16:25:47.924000Z', 'updated_at': '2023-02-12T16:25:49.183000Z'
            }})
        """
        # try:
        logger.info(f"Checking code: {code}, response: {response}")
        bulk_email_id = response['bulk_email_id']
        logger.info(f"Checking status of campaign: {self.campaign.id}, bulk_email_id: {bulk_email_id}")
        checked_n_amount_of_times = 0

        while checked_n_amount_of_times < MAILERSEND_MAX_NO_OF_BULK_STATUS_CHECKS:
            (code, response) = self.backend.get_bulk_status_by_id(bulk_email_id)
            logger.info(f"Checking bulk status Code: {code}, and Response: {response}")
            if "data" in response and "state" in response["data"] and response["data"]["state"] == BULK_STATE_COMPLETED:
                return (code, response) # bulk mailout is finished nothing else left to check, let's get out
            if not code and "message" in response and response['message'] == ERROR_CODE_BULK_EMAIL_NOT_FOUND:
                logger.exception(f"Unable to check back on the bulk email with bulk_email_id=\"{bulk_email_id}\"")
                raise MailerSendException(ERROR_MESSAGE_BULK_EMAIL_NOT_FOUND, ERROR_CODE_BULK_EMAIL_NOT_FOUND)
            checked_n_amount_of_times += 1
            messages.success(self.request, _("Sending initated via MailerSend (refresh the page to check the status)."))
            sleep(MAILERSEND_BULK_STATUS_CHECK_TIMEOUT) # give MailerSend a bit of time before checking the status again

        messages.success(self.request, _("Sending of the Campaign timed out"))
        logger.exception(f"Checking back on the bulk email with bulk_email_id=\"{bulk_email_id}\" timed out")
        raise MailerSendException(ERROR_MESSAGE_BULK_CHECK_TIMED_OUT, ERROR_CODE_BULK_CHECK_TIMED_OUT)
            #(None, {'data': {'id': '63ea1292723855014d07c40b', 'state': 'queued', 'total_recipients_count': 4, 'suppressed_recipients_count': 1, 'suppressed_recipients': {'63ea129288b712c3170c7e5d': {'to': [{'email': 'underlivaerable@raquel.yoga', 'name': None, 'reasons': ['on_hold']}]}}, 'validation_errors_count': 2, 'validation_errors': {'message.1': {'to.0.email': ['Recipient domain must match senders domain.']}, 'message.3': {'to.0.email': ['Recipient domain must match senders domain.']}}, 'messages_id': ['63ea129288b712c3170c7e5c', '63ea129288b712c3170c7e5d'], 'created_at': '2023-02-13T10:36:02.165000Z', 'updated_at': '2023-02-13T10:36:02.566000Z'}})

        # except:
        #     logger.exception(f"Problem with checking status of campaign: {self.campaign.id}")

    def seal_campaign(self, validation_errors):
        """
        Seals the self.campaign by generating its Contact receipts and marking its status as SENT.
        """
        unsuccessful_contacts = []
        for key, validation_error in validation_errors.items():
            try:
                contact_index = int(key.split('.')[1])
                contact = self.contacts[contact_index]
                logger.info(f"INFO: Couldn't send campaign to {contact.email} due to validation error: {validation_error}")
                unsuccessful_contacts.append(self.contacts[contact_index].id)
            except:
                logger.exception(f"Problem with parsing of a validation error with key=\"{key}\", value=\"{validation_error}\"")

        with transaction.atomic():
            Receipt.objects.bulk_create(
                [Receipt(campaign_id=self.campaign.id, contact_id=contact.id, success=(0 if contact.id in unsuccessful_contacts else 1))
                    for contact in self.contacts]
            )
            messages.success(self.request, _("Campaign sent to {} contacts successfully").format(len(self.contacts)-len(unsuccessful_contacts)))
            if len(unsuccessful_contacts) > 0:
                messages.warning(self.request, _("Unable to send campaign to {} contacts").format(len(unsuccessful_contacts)))
            self.campaign.status = CampaignStatus.SENT
            self.campaign.sent_date = timezone.now()
            self.campaign.save()


class MailersendEmailBackend(BaseEmailBackend):
    """
    Provides interface to the Mailersend API to test and send Campaigns to Contacts
    """

    def __init__(self):
        super().__init__()
        self.mailersend = emails.NewEmail(settings.BIRDSONG_MAILERSEND_API_KEY)

    def send_campaign(self, request, campaign, contacts, test_send=False):
        """
        Sends the campaign to contacts or the test contact (depending on the test_send paramater).

        :param request: HttpRequest - Request to be used to add Django messages
        :param campaign: Campaign - Emails of which to be sent out to Contacts
        :param contacts: List of Contacts - Recipients of the campaign's emails
        :param test_send: boolean - Determines whether to send campaign in test mode or not
        """

        email_messages = []
        for contact in contacts:
            content = render_to_string(
                campaign.get_template(request),
                campaign.get_context(request, contact),
            )
            email_messages.append(EmailMessage(
                subject=campaign.subject,
                body=content,
                from_email=self.from_email,
                to=[contact.email],
                reply_to=[self.reply_to],
            ))

        campaign_thread = SendCampaignThread(request, campaign, contacts, email_messages, test_send, self)
        campaign_thread.start()
        sleep(1) # HACK: Give Django Messages a little bit of time before returning back from initiating a campaign mail-out

    def get_email_list(self, email_messages):
        """
        Generates a list of email details from a dictionary of email_messages.
        TODO: Consider not using prepopulated list of EmailMessage objects and rather directly build email_messages
              as a list from campaign and contacts paramaters in send_campaign()
              The only argument against that is that by not using EmailMessage we would loose its inherent validation capabilities.

        :param email_messages: List of email dictionaries - e.g. [{"from": {"email": "test@example.com"}, ...}, ...]
        """
        mail_list = []
        for email_message in email_messages:
            encoding = email_message.encoding or settings.DEFAULT_CHARSET
            mail_list.append(
                {
                    "from": {
                        "email": sanitize_address(email_message.from_email, encoding)
                    },
                    "reply_to": {
                        "email": sanitize_address(self.reply_to, encoding)
                    },
                    "to": [dict(email=sanitize_address(addr, encoding)) for addr in email_message.to],
                    "subject": email_message.subject,
                    "text": "", # TODO: generate plaintext content
                    "html": email_message.body
                }
            )
        return mail_list

    def send_bulk(self, email_messages):
        """
        Initates asynchronous sending of email_messages via MailerSend API.
        Returned bulk_email_id can be afterwards passed to get_bulk_status_by_id() to check on the progress.

        :param email_messages: List of email dictionaries - @see get_email_list()
        :return: (int|void, dict) - e.g. (202, {'message': 'The bulk email is being processed.', 'bulk_email_id': '63dc744e837a822014066875'})
        """
        return self.parse_response(self.mailersend.send_bulk(self.get_email_list(email_messages)))
        # return (202, {'message': 'The bulk email is being processed.', 'bulk_email_id': '63ea1292723855014d07c40b'})
        # return self.parse_response('429\n{\n\t"message": "Daily API quota limit was reached."\n}')
        # text = "{\"message\":\"The bulk email is being processed.\",\"bulk_email_id\":\"63dc744e837a822014066875\"}"
        # text = "{\"message\":\"The bulk email is being processed.\",\"bulk_email_id\":\"63e9130bdcbf5643050513cb\"}"
        # return self.parse_response(f"202\n{text}")    
#         return self.parse_response("""403
# {
#     "message": "This action is unauthorized."
# }""")

    def get_bulk_status_by_id(self, bulk_email_id):
        """
        Retrieves the status of a bulk email identified by bulk_email_id parameter.

        :param bulk_email_id: string - e.g. "63ea1292723855014d07c40b"
        :return: (int|void, dict) - e.g. (None, {"message":"Resource not found."})
            or QUEUED state example:
            (None, {'data': {
                'id': '63e9130bdcbf5643050513cb', 'state': 'queued', 'total_recipients_count': 1,
                'suppressed_recipients_count': 0, 'suppressed_recipients': None,
                'validation_errors_count': 0, 'validation_errors': None,
                'messages_id': None,
                'created_at': '2023-02-12T16:25:47.924000Z', 'updated_at': '2023-02-12T16:25:47.924000Z'
            }})
            or COMPLETED state example:
            (None, {'data': {
                'id': '63e9130bdcbf5643050513cb', 'state': 'completed', 'total_recipients_count': 1,
                'suppressed_recipients_count': 0, 'suppressed_recipients': None,
                'validation_errors_count': 0, 'validation_errors': None,
                'messages_id': ['63e9130dc20a729ad4083df2'],
                'created_at': '2023-02-12T16:25:47.924000Z', 'updated_at': '2023-02-12T16:25:49.183000Z'
            }})
        """
        return self.parse_response(self.mailersend.get_bulk_status_by_id(bulk_email_id))
        # return (None, {'data': {'id': '63ea1292723855014d07c40b', 'state': 'queued', 'total_recipients_count': 4, 'suppressed_recipients_count': 1, 'suppressed_recipients': {'63ea129288b712c3170c7e5d': {'to': [{'email': 'underlivaerable@raquel.yoga', 'name': None, 'reasons': ['on_hold']}]}}, 'validation_errors_count': 2, 'validation_errors': {'message.1': {'to.0.email': ['Recipient domain must match senders domain.']}, 'message.3': {'to.0.email': ['Recipient domain must match senders domain.']}}, 'messages_id': ['63ea129288b712c3170c7e5c', '63ea129288b712c3170c7e5d'], 'created_at': '2023-02-13T10:36:02.165000Z', 'updated_at': '2023-02-13T10:36:02.566000Z'}})
        # return (None, {'data': {'id': '63e967350ee94e23c308236a', 'state': 'completed', 'total_recipients_count': 4, 'suppressed_recipients_count': 1, 'suppressed_recipients': {'63e967365ec84f8724074ce3': {'to': [{'email': 'underlivaerable@raquel.yoga', 'name': None, 'reasons': ['on_hold']}]}}, 'validation_errors_count': 2, 'validation_errors': {'message.1': {'to.0.email': ['Recipient domain must match senders domain.']}, 'message.3': {'to.0.email': ['Recipient domain must match senders domain.']}}, 'messages_id': ['63e967365ec84f8724074ce2', '63e967365ec84f8724074ce3'], 'created_at': '2023-02-12T22:24:53.707000Z', 'updated_at': '2023-02-12T22:24:54.384000Z'}})
        # return self.parse_response('{"message":"Resource not found."}')

    def parse_response(self, response):
        """
        Parses a rather unruly MailerSend response into a more python-friendly (int|void, dict) touple.

        :param response: string - e.g. '202\n'{"message":"The bulk email is being processed.","bulk_email_id":"63e9130bdcbf5643050513cb"}'
            or '429\n{\n\t"message": "Daily API quota limit was reached."\n}'
            or '{"message":"Resource not found."}'
        :return: (int|void, dict) - e.g. (202, {'message': 'The bulk email is being processed.', 'bulk_email_id': '63ea1292723855014d07c40b'})
            or (429, '{\n\t"message": "Daily API quota limit was reached."\n}')
            or (None, {"message":"Resource not found."})
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
            # logger.debug(response_split)
            # logger.debug(response_split[0])
            # logger.debug(response_split[1])
            (code, message) = (int(response_split[0]), json.loads(response_split[1]))
        except Exception as e:
            logger.exception(f"Problem with parsing of MailerSend's Response: {response}")
            raise e
        logger.info(f"Parsed to Code: {code}, Message: {message}")
        if code == ERROR_CODE_UNAUTHORIZED:
            raise MailerSendException(ERROR_MESSAGE_UNAUTHORIZED, code)
        if code == ERROR_CODE_DAILY_API_QUOTA_LIMIT_REACHED:
            # logger.exception(message['message'])
            # raise Exception(message['message'])
            raise MailerSendException(ERROR_MESSAGE_DAILY_API_QUOTA_LIMIT_REACHED, code) # Let's stop here, no point in proceeding further when out of API calls
        return (code, message)
