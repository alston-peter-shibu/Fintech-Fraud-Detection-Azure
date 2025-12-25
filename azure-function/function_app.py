import logging
import os
import azure.functions as func
from sendgrid import SendGridAPIClient
from sendgrid.helpers.mail import Mail
import json

# Initialize the Function App
app = func.FunctionApp()

# HTTP-triggered function for sending fraud alert emails
@app.route(route="sendFraudEmail", auth_level=func.AuthLevel.FUNCTION)
def sendFraudEmail(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Processing a fraud alert email request.')

    try:
        # Parse JSON request body
        req_body = req.get_json()
        customer_email = req_body.get('customer_email')  # Recipient email
        transaction_id = req_body.get('transaction_id')  # Transaction ID
        amount = req_body.get('amount')                  # Transaction amount
    except ValueError as e:
        logging.error(f"JSON parsing error: {str(e)}", exc_info=True)
        return func.HttpResponse("Invalid JSON", status_code=400)

    # Validate required fields
    if not customer_email or not transaction_id or not amount:
        logging.warning("Missing required fields")
        return func.HttpResponse("Missing required fields: customer_email, transaction_id, amount", status_code=400)

    # Retrieve SendGrid API key from environment variables
    sendgrid_key = os.environ.get('SENDGRID_API_KEY')  # <-- Replace in Azure Function App Settings
    if not sendgrid_key:
        logging.error("SENDGRID_API_KEY not set in environment variables")
        return func.HttpResponse("SendGrid API key not configured", status_code=500)

    # Prepare the email message
    message = Mail(
        from_email='YOUR_VERIFIED_SENDGRID_EMAIL@example.com',  # <-- Replace with your verified SendGrid sender email
        to_emails=customer_email,
        subject='Fraud Alert',
        html_content=f'<strong>Transaction Alert:</strong> Your transaction {transaction_id} for ${amount} is flagged as potentially fraudulent.'
    )

    # Send email via SendGrid
    try:
        sg = SendGridAPIClient(sendgrid_key)
        response = sg.send(message)
        logging.info(f"Email sent! Status code: {response.status_code}")
        return func.HttpResponse(f"Email sent! Status code: {response.status_code}")
    except Exception as e:
        logging.error(f"Failed to send email: {str(e)}", exc_info=True)
        return func.HttpResponse(f"Failed to send email: {str(e)}", status_code=500)
