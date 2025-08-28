import json
import logging
import os
import uuid
import base64
from datetime import datetime
from typing import Dict, Any
import io
from mistralai import Mistral
import PyPDF2

# Setup logging for AWS Lambda
logger = logging.getLogger()
logger.setLevel(logging.INFO)




def extract_text_fallback(pdf_data: bytes) -> str:
    """
    Fallback method for text extraction using basic parsing.
    
    Args:
        pdf_data: PDF file content as bytes
        
    Returns:
        Extracted text content
    """
    try:

        
        logger.info("Using fallback PyPDF2 text extraction...")
        
        pdf_file = io.BytesIO(pdf_data)
        pdf_reader = PyPDF2.PdfReader(pdf_file)
        
        extracted_text = ""
        for page_num in range(len(pdf_reader.pages)):
            page = pdf_reader.pages[page_num]
            extracted_text += page.extract_text() + "\n"
        
        logger.info(f"Fallback extraction completed: {len(extracted_text)} characters")
        return extracted_text
        
    except ImportError:
        logger.error("PyPDF2 not available for fallback extraction")
        return "OCR failed and no fallback method available"
    except Exception as e:
        logger.error(f"Fallback text extraction failed: {e}")
        return f"Text extraction failed: {str(e)}"


def extract_text_with_mistral_ocr(pdf_data: bytes, api_key: str) -> str:
    """
    Extract text from PDF using Mistral Document AI OCR client.
    
    Args:
        pdf_data: PDF file content as bytes
        api_key: Mistral API key
        
    Returns:
        Extracted text content
    """
    try:
        logger.info("Starting Mistral Document AI OCR processing...")
        
        # Encode PDF to base64
        base64_pdf = base64.b64encode(pdf_data).decode('utf-8')
        logger.info(f"Encoded PDF to base64 ({len(base64_pdf)} characters)")
        
        # Initialize Mistral client
        client = Mistral(api_key=api_key)
        
        logger.info("Making OCR request to Mistral API...")
        
        # Process document with OCR
        ocr_response = client.ocr.process(
            model="mistral-ocr-latest",
            document={
                "type": "document_url",
                "document_url": f"data:application/pdf;base64,{base64_pdf}" 
            },
            include_image_base64=True
        )
        
        # Extract text from the response
        # According to Mistral docs, OCR returns content in markdown format
        extracted_text = ""
        
        # The OCR response should contain the text content directly
        if hasattr(ocr_response, 'content'):
            extracted_text = ocr_response.content
        elif hasattr(ocr_response, 'text'):
            extracted_text = ocr_response.text
        elif hasattr(ocr_response, 'data'):
            # Some API responses wrap content in data field
            if hasattr(ocr_response.data, 'content'):
                extracted_text = ocr_response.data.content
            elif hasattr(ocr_response.data, 'text'):
                extracted_text = ocr_response.data.text
            else:
                extracted_text = str(ocr_response.data)
        else:
            # Convert entire response to string as fallback
            extracted_text = str(ocr_response)
        
        logger.info(f"Successfully extracted {len(extracted_text)} characters using Mistral OCR")
        return extracted_text
        
    except Exception as e:
        logger.error(f"Error during Mistral OCR processing: {e}")
        logger.info("Falling back to basic text extraction...")
        return extract_text_fallback(pdf_data)


def process_s3_file(s3_client, bucket_name: str, object_key: str, mistral_api_key: str) -> Dict[str, Any]:
    """
    Traite un fichier PDF spécifique dans S3 et extrait son contenu via Mistral OCR.
    
    Args:
        s3_client: Client boto3 S3 déjà initialisé
        bucket_name (str): Nom du bucket S3
        object_key (str): Nom/chemin du fichier dans le bucket
        mistral_api_key (str): Clé API Mistral
        
    Returns:
        dict: Résultats du traitement
    """
    print(f"📄 Traitement du PDF : s3://{bucket_name}/{object_key}")
    logger.info(f"Traitement du PDF : s3://{bucket_name}/{object_key}")
    
    # Vérifier que c'est bien un PDF
    if not object_key.lower().endswith('.pdf'):
        raise ValueError(f"Le fichier {object_key} n’est pas un PDF")
    
    # Vérifier le bon bucket (optionnel)
    target_bucket = os.getenv('TARGET_BUCKET', 'hubspot-tickets-pdf')
    if bucket_name != target_bucket:
        logger.warning(f"Traitement ignoré pour le bucket : {bucket_name} (attendu : {target_bucket})")
        return {
            'statusCode': 200,
            'body': json.dumps({
                'message': f'Traitement ignoré pour le bucket {bucket_name}',
                'processing_status': 'skipped'
            })
        }
    
    # Génération d’un ID unique
    document_id = str(uuid.uuid4())
    processing_timestamp = datetime.utcnow().isoformat() + 'Z'
    
    # Téléchargement du fichier depuis S3
    try:
        logger.info(f"Téléchargement du PDF depuis s3://{bucket_name}/{object_key}")
        response = s3_client.get_object(Bucket=bucket_name, Key=object_key)
        pdf_content = response['Body'].read()
        
        # Vérification de la taille pour Free Tier
        pdf_size_mb = len(pdf_content) / (1024 * 1024)
        if pdf_size_mb > 10:
            logger.warning(f"PDF trop volumineux ({pdf_size_mb:.1f}MB). Ignoré.")
            return {
                'statusCode': 200,
                'body': json.dumps({
                    'message': f'PDF trop volumineux ({pdf_size_mb:.1f}MB) - ignoré',
                    'processing_status': 'skipped_size_limit'
                })
            }
        
        logger.info(f"Téléchargement réussi : {len(pdf_content)} octets ({pdf_size_mb:.1f}MB)")
    except Exception as e:
        logger.error(f"Échec du téléchargement du PDF : {e}")
        raise
    
    # Extraction du texte
    logger.info("Extraction du texte avec Mistral OCR...")
    extracted_text = extract_text_with_mistral_ocr(pdf_content, mistral_api_key)
    
    if not extracted_text or not extracted_text.strip():
        logger.warning("Aucun texte n’a pu être extrait")
        extracted_text = "Aucun texte extrait"
    
    # Log + affichage
    print("="*30)
    print("TEXTE EXTRAIT DU PDF :")
    print("="*30)
    print(f"Document : {object_key}")
    print(f"Taille : {len(pdf_content)} octets")
    print(f"Longueur du texte : {len(extracted_text)} caractères")
    print("-"*30)
    
    display_text = extracted_text[:2000] + "..." if len(extracted_text) > 2000 else extracted_text
    print(display_text)
    print("="*30)
    
    logger.info("="*30)
    logger.info("TEXTE EXTRAIT DU PDF :")
    logger.info("="*30)
    logger.info(f"Document : {object_key}")
    logger.info(f"Taille : {len(pdf_content)} octets")
    logger.info(f"Longueur du texte : {len(extracted_text)} caractères")
    logger.info("-"*30)
    logger.info(display_text)
    logger.info("="*30)
    
    return {
        'statusCode': 200,
        'body': json.dumps({
            'document_id': document_id,
            'bucket': bucket_name,
            'object_key': object_key,
            'text_length': len(extracted_text),
            'processing_status': 'success',
            'texte_OCR':display_text
        })
    }



