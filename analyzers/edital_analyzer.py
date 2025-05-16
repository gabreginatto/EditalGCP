#!/usr/bin/env python3
"""
Edital Analyzer Module

This module provides functionality to analyze procurement documents (editals)
using Google's Vertex AI and Gemini models. It can process PDF files directly,
or extract PDFs from ZIP and RAR archives before analysis.

Usage:
    analyzer = EditalAnalyzer(project_id, location, model_id)
    await analyzer.initialize()
    result = await analyzer.analyze_file(file_path)
"""

import os
import asyncio
import logging
import traceback
import tempfile
import zipfile
import rarfile
import shutil
from pathlib import Path
from datetime import datetime
from typing import Dict, List, Any, Optional, Union, Tuple
import re
import time

# For PDF text extraction
import pdfplumber

# For Vertex AI and Gemini
try:
    from google.cloud import aiplatform
    from vertexai.preview.generative_models import GenerativeModel, Part
    HAS_VERTEX_AI = True
except ImportError:
    HAS_VERTEX_AI = False
    logging.warning("Vertex AI libraries not installed. Analysis functionality will be limited.")

# Configure logging
log_dir = os.path.join("downloads", "logs")
if not os.path.exists(log_dir) and os.path.exists("downloads"):
    os.makedirs(log_dir, exist_ok=True)

log_file = f"edital_analyzer_{datetime.now().strftime('%Y%m%d_%H%M%S')}.log"
log_path = os.path.join(log_dir, log_file) if os.path.exists(log_dir) else log_file

logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - [%(name)s] - %(message)s',
    handlers=[
        logging.FileHandler(log_path),
        logging.StreamHandler()
    ]
)
logger = logging.getLogger("EditalAnalyzer")

class EditalAnalyzer:
    """
    Class to analyze procurement documents (editals) using Vertex AI and Gemini models.
    """
    
    def __init__(self, project_id: str, location: str = "us-central1", 
                 model_id: str = "gemini-2.0-flash-lite-001",
                 output_dir: str = "analysis_results"):
        """
        Initialize the EditalAnalyzer with Google Cloud project settings.
        
        Args:
            project_id: Google Cloud Project ID
            location: Vertex AI location (default: us-central1)
            model_id: Gemini model ID (default: gemini-2.0-flash-lite-001)
            output_dir: Directory to save analysis results (default: analysis_results)
        """
        self.project_id = project_id
        self.location = location
        self.model_id = model_id
        self.output_dir = output_dir
        self.initialized = False
        self.model = None
        
        # Create output directory
        os.makedirs(output_dir, exist_ok=True)
        logger.info(f"Output directory set to: {output_dir}")
    
    async def initialize(self) -> bool:
        """
        Initialize Vertex AI for analysis.
        
        Returns:
            bool: True if initialization was successful, False otherwise
        """
        if not HAS_VERTEX_AI:
            logger.error("Vertex AI libraries not installed. Cannot initialize.")
            return False
            
        if self.initialized:
            logger.info("Vertex AI already initialized.")
            return True
            
        try:
            logger.info(f"Initializing Vertex AI with project: {self.project_id}, location: {self.location}")
            
            # This needs to run in a thread to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            await loop.run_in_executor(
                None, 
                lambda: aiplatform.init(project=self.project_id, location=self.location)
            )
            
            # Initialize the Gemini model
            self.model = await loop.run_in_executor(
                None,
                lambda: GenerativeModel(self.model_id)
            )
            
            logger.info(f"Model initialized: {self.model_id}")
            self.initialized = True
            return True
            
        except Exception as e:
            logger.error(f"Error initializing Vertex AI: {e}")
            logger.error(traceback.format_exc())
            return False
    
    async def analyze_file(self, file_path: str) -> Dict[str, Any]:
        """
        Analyze a procurement document file. Handles PDFs directly or extracts PDFs from archives.
        
        Args:
            file_path: Path to the file (PDF, ZIP, or RAR)
            
        Returns:
            Dict containing analysis results and metadata
        """
        if not self.initialized and not await self.initialize():
            return {
                "success": False,
                "original_file": file_path,
                "error": "Analyzer not initialized. Check Vertex AI credentials."
            }
            
        file_extension = os.path.splitext(file_path)[1].lower()
        
        try:
            # Handle different file types
            if file_extension == '.pdf':
                # Direct PDF analysis
                return await self._analyze_pdf(file_path)
                
            elif file_extension in ('.zip', '.rar'):
                # Archive file handling
                return await self._analyze_archive(file_path, file_extension)
                
            else:
                logger.warning(f"Unsupported file type: {file_extension}")
                return {
                    "success": False,
                    "original_file": file_path,
                    "error": f"Unsupported file type: {file_extension}"
                }
                
        except Exception as e:
            logger.error(f"Error analyzing file {file_path}: {e}")
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "original_file": file_path,
                "error": str(e)
            }
    
    async def _analyze_pdf(self, pdf_path: str) -> Dict[str, Any]:
        """
        Extract text from a PDF and analyze it with Gemini.
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Dict containing analysis results and metadata
        """
        logger.info(f"Analyzing PDF: {pdf_path}")
        
        # Extract text from PDF
        text = await self._extract_text(pdf_path)
        
        if not text:
            logger.warning(f"No text extracted from {pdf_path}")
            return {
                "success": False,
                "original_file": pdf_path,
                "error": "No text could be extracted from PDF"
            }
            
        # Analyze the text with Gemini
        analysis = await self._analyze_with_gemini(text)
        
        # Save analysis results
        pdf_name = os.path.basename(pdf_path)
        analysis_path = await self._save_analysis(analysis, pdf_name)
        
        return {
            "success": True,
            "original_file": pdf_path,
            "analysis_file": analysis_path,
            "analysis_text": analysis,
            "timestamp": datetime.now().isoformat()
        }
    
    async def _analyze_archive(self, archive_path: str, extension: str) -> Dict[str, Any]:
        """
        Extract PDFs from an archive file and analyze them.
        
        Args:
            archive_path: Path to the archive file
            extension: File extension ('.zip' or '.rar')
            
        Returns:
            Dict containing analysis results for all PDFs in the archive
        """
        logger.info(f"Analyzing archive: {archive_path}")
        
        # Create a temporary directory for extraction
        temp_dir = tempfile.mkdtemp(prefix="edital_analysis_")
        
        try:
            # Extract the archive based on file type
            pdf_files = []
            
            if extension == '.zip':
                pdf_files = await self._extract_zip(archive_path, temp_dir)
            elif extension == '.rar':
                pdf_files = await self._extract_rar(archive_path, temp_dir)
            
            if not pdf_files:
                logger.warning(f"No PDF files found in archive: {archive_path}")
                return {
                    "success": False,
                    "original_file": archive_path,
                    "error": "No PDF files found in archive"
                }
            
            # Analyze each PDF
            results = []
            for pdf_file in pdf_files:
                # Move the PDF file to the standard PDF directory
                base_dir = os.path.dirname(os.path.dirname(archive_path))  # Get parent directory of archive's parent
                pdf_dir = os.path.join(base_dir, "pdfs")
                
                # Make sure the PDF directory exists
                os.makedirs(pdf_dir, exist_ok=True)
                
                # Move the file with a unique name to avoid conflicts
                pdf_filename = os.path.basename(pdf_file)
                target_pdf_path = os.path.join(pdf_dir, f"{os.path.splitext(os.path.basename(archive_path))[0]}_{pdf_filename}")
                
                # Handle duplicate filenames
                if os.path.exists(target_pdf_path):
                    base, ext = os.path.splitext(pdf_filename)
                    target_pdf_path = os.path.join(pdf_dir, f"{os.path.splitext(os.path.basename(archive_path))[0]}_{base}_{int(time.time())}{ext}")
                
                # Copy the file instead of moving it to avoid issues with temporary directory cleanup
                shutil.copy2(pdf_file, target_pdf_path)
                logger.info(f"Copied extracted PDF to: {target_pdf_path}")
                
                # Analyze the file in its new location
                result = await self._analyze_pdf(target_pdf_path)
                results.append(result)
            
            # Get count of successful analyses
            successful = sum(1 for r in results if r["success"])
            
            # If multiple files were analyzed successfully, consolidate them
            consolidated_result = None
            if successful > 1:
                logger.info(f"Multiple analyses ({successful}) completed. Consolidating results...")
                consolidated_result = await self._consolidate_analyses(archive_path, results)
            
            # Return consolidated or individual results
            if consolidated_result:
                return {
                    "success": True,
                    "original_file": archive_path,
                    "file_type": "archive",
                    "extracted_files": len(pdf_files),
                    "successful_analyses": successful,
                    "consolidated": True,
                    "consolidated_file": consolidated_result.get("consolidated_file"),
                    "timestamp": datetime.now().isoformat()
                }
            else:
                return {
                    "success": successful > 0,
                    "original_file": archive_path,
                    "file_type": "archive",
                    "extracted_files": len(pdf_files),
                    "successful_analyses": successful,
                    "consolidated": False,
                    "results": results,
                    "timestamp": datetime.now().isoformat()
                }
            
        except Exception as e:
            logger.error(f"Error processing archive {archive_path}: {e}")
            logger.error(traceback.format_exc())
            return {
                "success": False,
                "original_file": archive_path,
                "error": str(e)
            }
        finally:
            # Clean up the temporary directory
            shutil.rmtree(temp_dir, ignore_errors=True)
    
    async def _consolidate_analyses(self, archive_path: str, results: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Consolidate multiple analysis results into a single comprehensive document.
        
        Args:
            archive_path: Path to the original archive file
            results: List of individual analysis results
            
        Returns:
            Dict containing the consolidated analysis
        """
        logger.info(f"Consolidating {len(results)} analysis results from {archive_path}")
        
        # Filter out failed analyses
        successful_results = [r for r in results if r.get("success", False)]
        
        if not successful_results:
            logger.warning("No successful analyses to consolidate")
            return None
        
        try:
            # Prepare data for consolidation
            consolidated_input = []
            files_to_delete = []
            
            for i, result in enumerate(successful_results, 1):
                analysis_text = result.get("analysis_text", "")
                file_path = result.get("analysis_file", "")
                original_file = os.path.basename(result.get("original_file", f"document_{i}"))
                
                if analysis_text and file_path:
                    # Add to consolidation input
                    consolidated_input.append({
                        "file_name": original_file,
                        "analysis": analysis_text
                    })
                    
                    # Add to files to delete after consolidation
                    files_to_delete.append(file_path)
            
            # Generate consolidated analysis with Gemini
            consolidated_text = await self._generate_consolidated_analysis(consolidated_input)
            
            if not consolidated_text:
                logger.error("Failed to generate consolidated analysis")
                return None
            
            # Save consolidated analysis with the improved naming convention
            archive_name = os.path.basename(archive_path)
            consolidated_path = await self._save_analysis(
                consolidated_text, 
                f"{archive_name}_consolidated"
            )
            
            if not consolidated_path:
                logger.error("Failed to save consolidated analysis")
                return None
            
            # Delete individual analysis files
            for file_path in files_to_delete:
                try:
                    os.remove(file_path)
                    logger.info(f"Deleted individual analysis file: {file_path}")
                except Exception as e:
                    logger.warning(f"Failed to delete file {file_path}: {e}")
            
            return {
                "success": True,
                "consolidated_file": consolidated_path,
                "num_files_consolidated": len(consolidated_input),
                "timestamp": datetime.now().isoformat()
            }
            
        except Exception as e:
            logger.error(f"Error consolidating analyses: {e}")
            logger.error(traceback.format_exc())
            return None
    
    async def _generate_consolidated_analysis(self, analyses: List[Dict[str, str]]) -> str:
        """
        Use Gemini to generate a consolidated analysis from multiple individual analyses.
        
        Args:
            analyses: List of dictionaries with file_name and analysis text
            
        Returns:
            Consolidated analysis text
        """
        if not analyses:
            return ""
            
        if not self.initialized or not self.model:
            logger.error("Gemini model not initialized")
            return ""
        
        try:
            logger.info(f"Generating consolidated analysis from {len(analyses)} documents...")
            
            # Format individual analyses for the prompt
            formatted_analyses = ""
            for i, analysis in enumerate(analyses, 1):
                formatted_analyses += f"\n\n--- DOCUMENT {i}: {analysis['file_name']} ---\n\n"
                formatted_analyses += analysis['analysis']
            
            # Consolidation prompt
            prompt = """
            # TAREFA: CONSOLIDAÇÃO DE ANÁLISES DE DOCUMENTOS DE LICITAÇÃO
            
            Você receberá análises de vários documentos relacionados a uma mesma licitação (Edital principal e seus anexos). Cada documento foi analisado separadamente, e agora você deve criar UMA ÚNICA ANÁLISE CONSOLIDADA E ABRANGENTE que combine todas as informações importantes.
            
            ## INSTRUÇÕES:
            
            1. Mantenha a mesma estrutura das análises individuais, com estas seções:
               - Cidade/Município
               - Empresa/Órgão responsável
               - Objeto da licitação
               - Especificações técnicas dos produtos/serviços
               - Valores estimados
               - Data de abertura
               - Prazo para envio de propostas
               - Requisitos para participação
               - Critérios de julgamento
            
            2. RESOLVA CONFLITOS:
               - Se informações diferentes aparecerem em documentos distintos, priorize a informação mais específica ou mais recente
               - Se não for possível determinar qual é a correta, inclua ambas indicando a fonte
            
            3. CONSOLIDE TABELAS:
               - Combine tabelas de itens de diferentes documentos
               - Se o mesmo item aparecer mais de uma vez, mantenha apenas uma entrada com a informação mais completa
               - Mantenha todas as informações de preços e quantidades
            
            4. NÃO REPITA INFORMAÇÕES:
               - Cada informação deve aparecer apenas uma vez na análise consolidada
               - Foque em criar um documento único que um leitor possa entender completamente
            
            5. PRESERVAÇÃO DE DADOS IMPORTANTES:
               - Assegure que TODOS os itens, quantidades, valores e especificações estejam na análise consolidada
               - Não omita informações relevantes presentes em qualquer um dos documentos originais
            
            6. FORMATAÇÃO:
               - Use Markdown para formatar o resultado
               - Utilize tabelas para apresentar dados tabulares
               - Mantenha a formatação clara e consistente
            
            7. ADICIONE UMA SEÇÃO DE SUMÁRIO no início do documento, antes das seções regulares
            
            ### DOCUMENTOS PARA CONSOLIDAÇÃO:
            """
            
            # Combine prompt with analyses
            full_prompt = prompt + formatted_analyses
            
            # Run in executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            response = await loop.run_in_executor(
                None,
                lambda: self.model.generate_content(
                    full_prompt,
                    generation_config={
                        "max_output_tokens": 4096,
                        "temperature": 0.2,
                        "top_p": 0.9,
                        "top_k": 40
                    }
                )
            )
            
            # Extract the response text
            if hasattr(response, 'text'):
                logger.info("Successfully generated consolidated analysis")
                return response.text
            else:
                logger.warning("Unexpected response format from consolidation request")
                return str(response)
                
        except Exception as e:
            logger.error(f"Error generating consolidated analysis: {e}")
            logger.error(traceback.format_exc())
            return f"Failed to generate consolidated analysis due to an error: {str(e)}"
    
    async def _extract_zip(self, zip_path: str, extract_dir: str) -> List[str]:
        """
        Extract PDF files from a ZIP archive.
        
        Args:
            zip_path: Path to the ZIP file
            extract_dir: Directory to extract files to
            
        Returns:
            List of paths to extracted PDF files
        """
        pdf_files = []
        
        try:
            # Run in executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            await loop.run_in_executor(None, lambda: self._extract_zip_sync(zip_path, extract_dir, pdf_files))
            
            logger.info(f"Extracted {len(pdf_files)} PDF files from {zip_path}")
            return pdf_files
            
        except Exception as e:
            logger.error(f"Error extracting ZIP file {zip_path}: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def _extract_zip_sync(self, zip_path: str, extract_dir: str, pdf_files: List[str]):
        """Synchronous helper for ZIP extraction"""
        with zipfile.ZipFile(zip_path, 'r') as zip_ref:
            for file_info in zip_ref.infolist():
                if file_info.filename.lower().endswith('.pdf'):
                    # Extract the PDF file
                    extracted_path = zip_ref.extract(file_info, extract_dir)
                    pdf_files.append(extracted_path)
                    
                # Check if there are nested ZIP files
                elif file_info.filename.lower().endswith('.zip'):
                    nested_zip_path = zip_ref.extract(file_info, extract_dir)
                    self._extract_zip_sync(nested_zip_path, extract_dir, pdf_files)
    
    async def _extract_rar(self, rar_path: str, extract_dir: str) -> List[str]:
        """
        Extract PDF files from a RAR archive.
        
        Args:
            rar_path: Path to the RAR file
            extract_dir: Directory to extract files to
            
        Returns:
            List of paths to extracted PDF files
        """
        pdf_files = []
        
        try:
            # Run in executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            await loop.run_in_executor(None, lambda: self._extract_rar_sync(rar_path, extract_dir, pdf_files))
            
            logger.info(f"Extracted {len(pdf_files)} PDF files from {rar_path}")
            return pdf_files
            
        except Exception as e:
            logger.error(f"Error extracting RAR file {rar_path}: {e}")
            logger.error(traceback.format_exc())
            return []
    
    def _extract_rar_sync(self, rar_path: str, extract_dir: str, pdf_files: List[str]):
        """Synchronous helper for RAR extraction"""
        with rarfile.RarFile(rar_path, 'r') as rar_ref:
            for file_info in rar_ref.infolist():
                if file_info.filename.lower().endswith('.pdf'):
                    # Extract the PDF file
                    extracted_path = rar_ref.extract(file_info, extract_dir)
                    pdf_files.append(extracted_path)
                    
                # Check if there are nested RAR files
                elif file_info.filename.lower().endswith(('.rar', '.zip')):
                    nested_archive_path = rar_ref.extract(file_info, extract_dir)
                    
                    if nested_archive_path.lower().endswith('.rar'):
                        self._extract_rar_sync(nested_archive_path, extract_dir, pdf_files)
                    elif nested_archive_path.lower().endswith('.zip'):
                        self._extract_zip_sync(nested_archive_path, extract_dir, pdf_files)
    
    async def _extract_text(self, pdf_path: str) -> str:
        """
        Extract text from a PDF file using pdfplumber (async wrapper).
        
        Args:
            pdf_path: Path to the PDF file
            
        Returns:
            Extracted text as a string
        """
        logger.info(f"Extracting text from {pdf_path}...")
        
        try:
            # Run in executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            text = await loop.run_in_executor(None, lambda: self._extract_text_sync(pdf_path))
            
            if text:
                logger.info(f"Successfully extracted {len(text)} characters of text")
            else:
                logger.warning(f"No text extracted from {pdf_path}")
                
            return text
            
        except Exception as e:
            logger.error(f"Error extracting text from {pdf_path}: {e}")
            logger.error(traceback.format_exc())
            return ""
    
    def _extract_text_sync(self, pdf_path: str) -> str:
        """Synchronous helper for text extraction"""
        with pdfplumber.open(pdf_path) as pdf:
            text = ""
            for i, page in enumerate(pdf.pages):
                extracted = page.extract_text()
                if extracted:
                    text += extracted + "\n"
                else:
                    logger.warning(f"No text extracted from page {i+1}")
            
            return text.strip()
    
    async def _analyze_with_gemini(self, text: str) -> str:
        """
        Send text to Gemini model via Vertex AI for analysis.
        
        Args:
            text: Text to analyze
            
        Returns:
            Analysis result as a string
        """
        if not text:
            return "No text available for analysis."
        
        if not self.initialized or not self.model:
            logger.error("Gemini model not initialized")
            return "Error: Gemini model not initialized"
        
        try:
            logger.info(f"Analyzing text with Gemini model: {self.model_id}...")
            
            # Updated prompt for table extraction
            prompt = """
            Analise este documento de licitação e extraia as seguintes informações, priorizando tabelas e listas que contenham descrições de itens, quantidades e unidades. Formate a resposta de forma clara e organizada, com cada item em uma seção separada. Se alguma informação não estiver disponível no documento, indique "Não especificado".

            IMPORTANTE: Procure especificamente na seção "ANEXO I - TERMO DE REFERÊNCIA" ou "TERMO DE REFERÊNCIA" que geralmente começa após a página 20 do documento. Esta seção contém as tabelas com as especificações detalhadas dos produtos.

            1. Cidade/Município onde será realizada a licitação
            2. Empresa/Órgão responsável pela licitação
            3. Objeto da licitação (o que está sendo licitado)
            4. Especificações técnicas dos produtos/serviços (incluindo detalhes de tabelas como descrições, quantidades, e unidades, organizados por lote se aplicável)
            5. Valores estimados ou de referência (se disponíveis em tabelas ou texto)
            6. Data de abertura da licitação
            7. Prazo para envio de propostas
            8. Requisitos para participação
            9. Critérios de julgamento das propostas

            Para os itens 4 e 5, procure especificamente por tabelas ou listas que detalhem:
            - Descrição do item (e.g., "Tubete Especial Curto Oitavado", "Porca Sextavada")
            - Quantidade (e.g., "2.000", "1.000")
            - Unidade (e.g., "Peça")
            - Organize essas informações em tabelas no formato:
              | ITEM | DESCRIÇÃO                     | QUANTIDADE | UND   |
              |------|-------------------------------|------------|-------|
              | 01   | Tubete Especial Curto Oitavado| 2.000      | Peça  |
              | 02   | Porca Sextavada              | 2.000      | Peça  |
            Se os dados estiverem espalhados ou não em tabelas, compile-os da melhor forma possível em um formato tabular.

            INSTRUÇÕES ESPECÍFICAS:
            1. Para as especificações técnicas (item 4), seja conciso e liste apenas as características principais de cada item, evitando detalhes excessivos.
            2. Para os valores estimados (item 5), além de mostrar os valores por lote, adicione uma linha ao final com o VALOR TOTAL GERAL somando todos os lotes.
            3. Formate a resposta em Markdown para melhor legibilidade.

            Documento:
            """
            
            # Increase the character limit to include more of the document
            max_chars = 50000  # Limit text size to avoid API limitations
            if len(text) > max_chars:
                # Take the first 20000 characters and the last 30000 characters to capture both header info and the Termo de Referência
                first_part = text[:20000]
                last_part = text[-30000:] if len(text) > 30000 else text[20000:]
                text = first_part + "\n...[texto intermediário omitido]...\n" + last_part
            
            # Run in executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            response = await loop.run_in_executor(
                None,
                lambda: self.model.generate_content(
                    prompt + text,
                    generation_config={
                        "max_output_tokens": 2048,
                        "temperature": 0.2,
                        "top_p": 0.9,
                        "top_k": 40
                    }
                )
            )
            
            # Extract the response text
            if hasattr(response, 'text'):
                return response.text
            else:
                logger.warning("Unexpected response format")
                return str(response)
                
        except Exception as e:
            logger.error(f"Error analyzing text with Gemini: {e}")
            logger.error(traceback.format_exc())
            return f"Analysis failed due to an error: {str(e)}"
    
    async def _save_analysis(self, analysis: str, pdf_name: str) -> str:
        """
        Save the analysis to a file with a meaningful name based on analysis content.
        
        Args:
            analysis: Analysis text
            pdf_name: Name of the original PDF
            
        Returns:
            Path to the saved analysis file
        """
        # Extract location, object, and date from the analysis if available
        location = "Unknown"
        procurement_object = "Undefined"
        date = datetime.now().strftime("%Y%m%d")
        
        # Try to extract location from analysis
        location_match = re.search(r'(?:Cidade/Município|Município|Cidade)[:\s]+([\w\s]+)', analysis)
        if location_match:
            location = location_match.group(1).strip()[:30]  # Limit length
        
        # Try to extract object from analysis
        object_match = re.search(r'(?:Objeto da licitação|Objeto)[:\s]+([\w\s,\.;]+)', analysis)
        if object_match:
            procurement_object = object_match.group(1).strip()[:50]  # Limit length
        
        # Try to extract date from analysis
        date_match = re.search(r'(?:Data de abertura|Data)[:\s]+(\d{2}[/.-]\d{2}[/.-]\d{2,4})', analysis)
        if date_match:
            date_text = date_match.group(1).strip()
            # Convert date to YYYYMMDD format if possible
            try:
                # Try different date formats
                for date_format in ('%d/%m/%Y', '%d-%m-%Y', '%d.%m.%Y', '%d/%m/%y', '%d-%m-%y', '%d.%m.%y'):
                    try:
                        parsed_date = datetime.strptime(date_text, date_format)
                        date = parsed_date.strftime("%Y%m%d")
                        break
                    except ValueError:
                        continue
            except Exception as e:
                logger.warning(f"Could not parse date '{date_text}': {e}")
        
        # Clean strings for filename
        location = re.sub(r'[\\/*?:"<>|]', '-', location)
        procurement_object = re.sub(r'[\\/*?:"<>|]', '-', procurement_object)
        
        # Create the filename
        analysis_name = f"{location}_{procurement_object}_{date}.md"
        # Replace multiple spaces and special characters
        analysis_name = re.sub(r'\s+', '_', analysis_name)
        analysis_name = re.sub(r'_{2,}', '_', analysis_name)
        
        # Fallback to original naming if we couldn't extract meaningful information
        if analysis_name.startswith("Unknown_Undefined_"):
            timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
            analysis_name = f"analysis_{pdf_name.replace('.pdf', '')}_{timestamp}.md"
        
        output_file = os.path.join(self.output_dir, analysis_name)
        
        try:
            # Run in executor to avoid blocking the event loop
            loop = asyncio.get_event_loop()
            
            await loop.run_in_executor(
                None,
                lambda: self._save_file_sync(output_file, analysis)
            )
            
            logger.info(f"Analysis saved to {output_file}")
            return output_file
            
        except Exception as e:
            logger.error(f"Error saving analysis: {e}")
            logger.error(traceback.format_exc())
            return ""
    
    def _save_file_sync(self, file_path: str, content: str):
        """Synchronous helper for file saving"""
        with open(file_path, "w", encoding="utf-8") as f:
            f.write(content)


async def test_analyzer():
    """Simple test function for the EditalAnalyzer"""
    from dotenv import load_dotenv
    load_dotenv()
    
    project_id = os.getenv("GOOGLE_CLOUD_PROJECT_ID")
    if not project_id:
        print("Error: GOOGLE_CLOUD_PROJECT_ID not set in environment")
        return
    
    pdf_path = input("Enter path to a PDF file or archive to test: ")
    if not os.path.exists(pdf_path):
        print(f"Error: File not found: {pdf_path}")
        return
    
    analyzer = EditalAnalyzer(project_id)
    await analyzer.initialize()
    
    result = await analyzer.analyze_file(pdf_path)
    
    print("\nAnalysis Result:")
    print(f"Success: {result['success']}")
    
    if result['success']:
        if 'consolidated' in result and result['consolidated']:
            print(f"Consolidated analysis of {result['successful_analyses']} files saved to: {result['consolidated_file']}")
        elif 'analysis_file' in result:
            print(f"Analysis saved to: {result['analysis_file']}")
        else:
            print(f"Analyzed {result.get('successful_analyses', 0)} files in archive")
    else:
        print(f"Error: {result.get('error', 'Unknown error')}")


if __name__ == "__main__":
    import asyncio
    asyncio.run(test_analyzer())