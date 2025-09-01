from tools import *
import os

import json
import logging
import os
from datetime import datetime
from mistralai import Mistral
from dotenv import load_dotenv


logger = logging.getLogger()
logger.setLevel(logging.INFO)


def lambda_handler(event, context):
    """
    Handler AWS Lambda pour le traitement des fichiers PDF avec Mistral OCR.
    """

    # ----------------------------------------------------------->
    # (1) Structure log pour un fichier PDF.
    log_data = {
        "metadata": {
            "file_name": "",
            "created_at": datetime.utcnow().isoformat() + "Z",
        },
        "workflow": {
            
            "OCR":  {"status": "Not started", "details": "", "data": {}},
            "LLM":  {"status": "Not started", "details": "", "data": {}},
            "DEAL": {"status": "Not started", "details": "", 
                     
                "transaction": {"dealname": "","id_deal": ""},
                "matching_company":{},
                "matching_products":{}
            },
        },

    }      
    # ----------------------------------------------------------->

    # (2) Bucket S3.
    bucket_name = "hubspot-tickets-pdf"

    try:
        print("🚀 Lambda function started")
        logger.info("Lambda function started")

        # (3) Validation des variables d’environnement.
        load_dotenv()
        mistral_api_key = os.getenv("MISTRAL_API_KEY")
        if not mistral_api_key:
            raise ValueError("MISTRAL_API_KEY environment variable is required")
        print(f"✅ Mistral API key found: {mistral_api_key[:10]}...")

        # (4) Connexion AWS.
        aws_conn = connexion_aws()
        if aws_conn["status"] != "success":
            raise RuntimeError("Connexion AWS échouée")
        s3_client = aws_conn["client"]

        # (5) 📄 Récupérer le dernier PDF.
        pdf_key = get_last_pdf(s3_client, bucket_name, prefix="PDF_TEST/")
        print(f"📄 Dernier PDF trouvé : {pdf_key}")

        # (6) Enregistrement du nom du fichier PDF dans le logging.
        file_name = pdf_key.replace("PDF_TEST/","")
        log_data["metadata"]["file_name"] = file_name

        # (7) Lancement de l’OCR.
        ocr_response = process_s3_file(
            s3_client       = s3_client,
            bucket_name     = bucket_name,
            object_key      = pdf_key,
            mistral_api_key = mistral_api_key
        )
        ocr_response = json.loads(ocr_response["body"])

        # (8) Vérification du résultat OCR.
        if ocr_response.get("processing_status") == "success":
            texte_ocr = ocr_response.get("texte_OCR", "")
            if not texte_ocr:
                
                # ----------------------------------------------------------->
                # (8.1) Logging d’erreur si aucun texte n’a été extrait.
                log_data["workflow"]["OCR"]["status"]  = "Failed"
                log_data["workflow"]["OCR"]["details"] = "Aucun texte extrait"
                logger.warning("Aucun texte OCR trouvé dans la réponse")
                return {"statusCode": 404,"body": json.dumps({"status": "failed","message": "Aucun texte extrait"})}
                # ----------------------------------------------------------->
                
            else:
                
                # ----------------------------------------------------------->
                # (8.2) Logging de succès si texte trouvé.
                log_data["workflow"]["OCR"]["status"]  = "Success"
                log_data["workflow"]["OCR"]["details"] = f"Processed"
                log_data["workflow"]["OCR"]["data"]    = texte_ocr
                print(f"✅ Successfully processed PDF: {ocr_response}")
                logger.info(f"Successfully processed PDF: {ocr_response}")
                # ----------------------------------------------------------->
        else:
            
            # ----------------------------------------------------------->
            # (8.3) Logging d’échec si OCR en erreur.
            log_data["workflow"]["OCR"]["status"]  = "Failed"
            log_data["workflow"]["OCR"]["details"] = ocr_response["message"]
            logger.error(f"OCR failed: {ocr_response}")
            return {"statusCode": 404,"body": json.dumps({"status": "failed","message":ocr_response["message"]})}
            # ----------------------------------------------------------->

        # ----------------------------------------------------------->
        # (9) Sauvegarde du log JSON dans S3 & résultat OCR.
        save_log_to_s3(s3_client=s3_client, bucket_name=bucket_name, log_data=log_data, file_name=file_name)
        save_file_OCR(s3_client=s3_client, response=ocr_response, bucket_name=bucket_name)
        # ----------------------------------------------------------->

        # (10) ✅ Retour de la fonction Lambda.
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Traitement terminé",
                "log": log_data
            })
        }

    except Exception as e:
        # ----------------------------------------------------------->
        # (11) Gestion d’erreur.
        log_data["workflow"]["OCR"]["status"]  = "Failed"
        log_data["workflow"]["OCR"]["details"] = str(e)
        logger.error(f"Error processing PDF: {str(e)}", exc_info=True)

        # sauvegarde du log en cas d’exception.
        s3_client = locals().get("s3_client", None)
        save_log_to_s3(s3_client=s3_client, bucket_name=bucket_name, log_data=log_data, file_name=file_name)

        return {
            "statusCode": 500,
            "body": json.dumps({
                "message": "Erreur lors du traitement",
                "error": str(e),
                "log"  : log_data
            })
        }
        # ----------------------------------------------------------->