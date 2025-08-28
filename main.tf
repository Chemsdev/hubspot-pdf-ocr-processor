provider "aws" {
  region = "eu-north-1"
}

# Récupération dynamique des dernières versions des layers existants
data "aws_lambda_layer_version" "python-dotenv" {
  layer_name = "python-dotenv"
}

data "aws_lambda_layer_version" "mistralai" {
  layer_name = "mistralai"
}

data "aws_lambda_layer_version" "pdf2image" {
  layer_name = "pdf2image"
}

# Création du fichier ZIP de la fonction Lambda
data "archive_file" "lambda_zip" {
  type        = "zip"
  source_dir  = "${path.module}/lambda_function"
  output_path = "${path.module}/lambda_function.zip"
}

# Déploiement de la fonction Lambda avec layers dynamiques
resource "aws_lambda_function" "hubspot-pdf-ocr-processor" {
  function_name = " hubspot-pdf-ocr-processor"
  handler       = " hubspot-pdf-ocr-processor.lambda_handler"
  runtime       = "python3.9"
  role          = "arn:aws:iam::975515885951:role/lambda"
  filename         = data.archive_file.lambda_zip.output_path
  source_code_hash = data.archive_file.lambda_zip.output_base64sha256

  timeout = 900

  # Variables de la base de données.
    environment {
    variables = {
        MISTRAL_API_KEY = var.mistral_api_key
    }
    }


  # Layers de la Lambda.   
  layers = [
    data.aws_lambda_layer_version.mistralai.arn,
    data.aws_lambda_layer_version.pdf2image.arn,
    data.aws_lambda_layer_version.python-dotenv.arn
  ]
}


