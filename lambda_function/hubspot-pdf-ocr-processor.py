from tools import *
import os

import json
import logging
import os
from datetime import datetime
from mistralai import Mistral



def lambda_handler(event, context):
    """
    Handler AWS Lambda pour le traitement des fichiers PDF avec Mistral OCR.
    """
    bucket_name = "hubspot-tickets-pdf"
    try:
        print("🚀 Lambda function started")
        logger.info("Lambda function started")
        
        # Validate environment variables
        mistral_api_key = os.getenv("MISTRAL_API_KEY")
        if not mistral_api_key:
            raise ValueError("MISTRAL_API_KEY environment variable is required")
        
        print(f"✅ Mistral API key found: {mistral_api_key[:10]}...")
        
        # Connexion AWS.
        aws_conn = connexion_aws()
        if aws_conn["status"] != "success":
            raise RuntimeError("Connexion AWS échouée")
        s3_client = aws_conn["client"]
        
        # 📄 Récupérer le dernier PDF dans le dossier PDF/.
        pdf_key = get_last_pdf(s3_client, bucket_name, prefix="PDF_TEST/")
        print(f"📄 Dernier PDF trouvé : {pdf_key}")
        
        # Lancement de l'OCR.
        response = process_s3_file(
            s3_client       = s3_client,
            bucket_name     = bucket_name,
            object_key      = pdf_key,
            mistral_api_key = mistral_api_key
        )
        
        # Enregistrement du résultat OCR dans un fichier texte : OCR_nom_pdf.txt
        save_file_OCR(s3_client=s3_client, response=response, bucket_name=bucket_name)

        print(f"✅ Successfully processed PDF: {response}")
        logger.info(f"Successfully processed PDF: {response}")
        return response
        
    except Exception as e:
        logger.error(f"Error processing PDF: {str(e)}", exc_info=True)
        return {
            'statusCode': 500,
            'body': json.dumps({
                'error': str(e),
                'processing_status': 'failed'
            })
        }
