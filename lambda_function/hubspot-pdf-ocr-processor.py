from tools import *
from dotenv import load_dotenv
import boto3
import os

import json
import logging
import os
from datetime import datetime
from mistralai import Mistral

# Setup logging for AWS Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)

AWS_CONNEXION_DATALO = [
    "ACCESS_KEY_ID_CHEMS",   
    "SECRET_ACCESS_KEY_CHEMS",
    "REGION_CHEMS"      
]

# Fonction permettent de se connecter à AWS. 
def connexion_aws(liste_connexion=AWS_CONNEXION_DATALO):
    try:
        load_dotenv()
        s3_client = boto3.client(
            's3',
            aws_access_key_id     = os.environ.get(liste_connexion[0]),
            aws_secret_access_key = os.environ.get(liste_connexion[1]),
            region_name           = os.environ.get(liste_connexion[2])
        )
        
        message = f"Connexion AWS réussie (région : {os.environ.get(liste_connexion[2])})."
        print(message)
        
        return {
            "status"  : "success",
            "message" : message,
            "client"  : s3_client
        }

    except Exception as e:
        error_message = f"Échec de la connexion AWS : {e}"
        print(error_message)

        return {
            "status"  : "error",
            "message" : error_message,
            "client"  : None
        }



# Création d'un fichier texte contenant la sortie OCR.
def save_file_OCR(s3_client, response: dict, bucket_name:str):
    """
    Sauvegarde le résultat OCR dans un fichier texte sur S3
    sous le dossier PDF_OCR/ avec un nom OCR_nom_pdf.txt.
    
    Args:
        s3_client: client boto3 S3 déjà connecté
        response (dict): contient au moins "texte_OCR" et "object_key"
        bucket_name (str): bucket cible pour stocker le fichier OCR
    """
    try:
        
        # Récupération des informations de l'OCR.
        response_body = response["body"]
        response_dict = json.loads(response_body)
        
        # Extraire le texte OCR depuis la réponse
        texte_ocr = response_dict.get("texte_OCR", "")
        if not texte_ocr:
            logger.warning("Aucun texte OCR trouvé dans la réponse")
            texte_ocr = "Aucun texte extrait"

        # Extraire le nom de base du PDF (ex: "4430_0" à partir de "PDF/4430_0.pdf")
        object_key      = response_dict.get("object_key", "")
        base_name       = os.path.basename(object_key).replace(".pdf", "")
        output_filename = f"OCR_{base_name}.txt"
        s3_output_key   = f"PDF_OCR/{output_filename}"

        # Upload du contenu OCR vers S3
        s3_client.put_object(
            Bucket=bucket_name,
            Key=s3_output_key,
            Body=texte_ocr.encode("utf-8"),
            ContentType="text/plain"
        )

        logger.info(f"✅ Résultat OCR enregistré sur S3 : s3://{bucket_name}/{s3_output_key}")
        print(f"✅ Résultat OCR enregistré sur S3 : s3://{bucket_name}/{s3_output_key}")

        return {
            "status": "success",
            "s3_path": f"s3://{bucket_name}/{s3_output_key}"
        }

    except Exception as e:
        error_message = f"Erreur lors de l'enregistrement du fichier OCR sur S3 : {e}"
        logger.error(error_message, exc_info=True)
        return {
            "status": "error",
            "message": error_message
        }




def lambda_handler(event=None, context=None):
    """
    Handler AWS Lambda pour le traitement des fichiers PDF avec Mistral OCR.
    Il écoute le bucket hubspot-tickets-pdf et journalise le texte extrait.
    
    Args:
        event: Événement AWS Lambda déclenché par S3
        context: Contexte d’exécution AWS Lambda
        
    Returns:
        dict: Réponse contenant les résultats du traitement
    """
    
    bucket_name = "hubspot-tickets-pdf"
    try:
        print("🚀 Lambda function started")
        logger.info("Lambda function started")
        
        # Validate environment variables
        mistral_api_key = "sY7feK9i4fppJqXcb3vuH8R8W6fozH5I"
        if not mistral_api_key:
            raise ValueError("MISTRAL_API_KEY environment variable is required")
        
        print(f"✅ Mistral API key found: {mistral_api_key[:10]}...")
        
        # Connexion AWS.
        aws_conn = connexion_aws()
        if aws_conn["status"] == "success":
            s3_client = aws_conn["client"]
        
        # Lancement de l'OCR.
        response = process_s3_file(
            s3_client       = s3_client,
            bucket_name     = bucket_name,
            object_key      = "PDF/4430_0.pdf",
            mistral_api_key = os.getenv("MISTRAL_API_KEY")
        )
        
        # Enregistrement du résultat de l'OCR dans un fichier texte : OCR_nom_pdf.txt
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
        
        
    