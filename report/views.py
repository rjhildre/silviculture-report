
import os
from django.template.loader import get_template, render_to_string
from django.http import HttpResponse 
from django.shortcuts import render, get_object_or_404
from django.template import Context
import logging
import pdfkit #https://ourcodeworld.com/articles/read/241/how-to-create-a-pdf-from-html-in-django
from report.queries import get_fma_details, get_overlapping_fma_details, \
get_regen_details, get_material_details, get_survey_details, get_index_form_values

# Oracle database connection configuration, set as environmental variables.
ORACLE_DATABASE = os.environ.get('ROPA')
ORACLE_USERNAME = os.environ.get('ROPA_USERNAME')
ORACLE_PASSWORD = os.environ.get('ROPA_PASSWORD')

logger = logging.getLogger('file_logger')
def index(request):
  '''
  Query ropa to get a list of timbersale names/id's and to FMA names/ids 
  to populate the selection tables of the index page. For timbersales, we only
  want ones that are planned or sold. For FMAs we only want timber harvest 
  activities that are planned. 
  '''
  try:
    timber_sales, fmas = get_index_form_values(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD)
  except:
    logger.debug("The ROPA query for the index page failed.")
  return render(request, 'report/index.html', {'timber_sales' : timber_sales, 'fmas' : fmas})
  
    

def timber_sale_report(request):
  """
  This view uses queries from queries.py to retrieve the information we need from Oracle
  that is specifict to the timber sale report. The timber sale report might contain several
  harvest activities, each with it's own set of overlapping activities. 
  It then uses PDFKit to render the PDF to the client. 
  """

  # Retrieve the timber sale name and id from the GET request.
  try: 
    timber_sale=request.GET.get('timber-sale')
    timber_sale_name = timber_sale.split(' -')[0]
    timber_sale_id = timber_sale.split(' - ')[1]
  except:
     logger.debug("The timber sale report page did not receive a valid timber" +
                  "sale name-id")

  try:
    # Create a list of FMAs, and their details, that overlap the timber harvest FMAs
    overlapping_fmas = get_overlapping_fma_details(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD, timber_sale_name)
    # Get the info for overlapping fmas that have regen activity (i.e. they hit the REGEN table)
    regen = get_regen_details(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD, timber_sale_name)
    #Get the info for the overlapping fmas that have managment activities (i.e. they hit the MATERIAL table)
    material = get_material_details(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD, timber_sale_name)
    # Get the info for the overlapping fmas that have survey activities (i.e. they hit the SURVEY table)
    survey = get_survey_details(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD, timber_sale_name)
    # Get the details for each fma in the timber sale
    fma_details = get_fma_details(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD, timber_sale_name)
  except:
    logger.debug("One of the ROPA queries for the timber-sale-report failed.")

  # Get a list of the fmas that have regen activity so that we can only add tables if needed. 
  needs_regen_table = [row[0] for row in regen]
  # Same for materials
  needs_materials_table = [row[0] for row in material]
  # And survey activities
  needs_survey_table = [row[0] for row in survey]

  # These are the parameters that are passed to the Django render method to populate
  # the timber-sale-report.html templates. 
  params = {
    'overlapping_fmas' : overlapping_fmas,
    'fma_details' : fma_details,
    'timber_sale' : timber_sale,
    'regen' : regen,
    'material' : material,
    'survey' : survey,
    'needs_regen_table' : needs_regen_table,
    'needs_materials_table' : needs_materials_table,
    'needs_survey_table' : needs_survey_table
  }

  # wkhtmltopdf options for creating the PDF. Tons of options here:
  # https://wkhtmltopdf.org/usage/wkhtmltopdf.txt
  options = {
    'print-media-type' : '',
    'page-size' : 'Letter',
    'footer-center':'[page] of [topage]',
    'footer-right':'Report Generated [date]'
  }

  # Tell pdfkit where to find the css files to style the report
  css = [
    r'silvreport/static/report/style.css'
  ]

  html_template = get_template('report/timber-sale-report.html')
  html = html_template.render(params)
  
  # Render the PDF
  try:
    path_to_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
    config = pdfkit.configuration(wkhtmltopdf=path_to_wkhtmltopdf)
    pdf = pdfkit.from_string(html, False, css=css, options=options, configuration=config)
    response = HttpResponse(pdf, content_type = 'application/pdf')
    response['Content-Disposition'] = f'attachment; filename="{timber_sale_name}.pdf"'
  except:
    logger.debug("The timber-sale-report queries ran but wkhtmltopdf failed to render the PDF.")
  return response

  """ For testing changes to page layout, it is usually easier to view the page
  in html in a web browser, instead of rendering a PDF every time. To do this
  simply comment out the six lines above, and uncomment the line below.  """
  #return render(request, 'report/timber-sale-report.html', params)

def fma_report(request):
  """
  This view is almost identical to the one above, however now we are just looking
  at one harvest activity, instead of a timber sale that might consist of several
  harvest activities. 
  """
  try:
    # Get the fma name and id from the GET request. 
    fma = request.GET.get('fma')
    fma_id = fma.split(' - ')[1]
    fma_name = fma.split(' - ')[0]
  except:
    logger.debug("The fma-report page did not recieve a valid fma name-id from the" +
                  "index page.")

  try:
    # Run the Oracle queries. 
    fma_details = get_fma_details(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD, fma_id, ts=False)
    overlapping_fmas = get_overlapping_fma_details(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD, fma_id, ts=False)
    regen = get_regen_details(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD, fma_id, ts=False)
    material = get_material_details(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD, fma_id, ts=False)
    survey = get_survey_details(ORACLE_DATABASE, ORACLE_USERNAME, ORACLE_PASSWORD, fma_id, ts=False)
  except:
    logger.debug("One fo the ROPA queries for the fma-report page failed.")

  # Get a list of the fmas that have regen activity so that we can only add tables if needed. 
  needs_regen_table = [row[0] for row in regen]
  # Same for materials
  needs_materials_table = [row[0] for row in material]
  # And survey activities
  needs_survey_table = [row[0] for row in survey]

  # Pass in the parameters
  params = {
    'overlapping_fmas' : overlapping_fmas,
    'fma_details' : fma_details,
    'regen' : regen,
    'material' : material,
    'survey' : survey,
    'needs_regen_table' : needs_regen_table,
    'needs_materials_table' : needs_materials_table,
    'needs_survey_table' : needs_survey_table
  }
# wkhtmltopdf options. Tons of options here:
  # https://wkhtmltopdf.org/usage/wkhtmltopdf.txt
  options = {
    'print-media-type' : '',
    'page-size' : 'Letter',
    'footer-center':'[page] of [topage]',
    'footer-right':'Report Generated [date]'
  }

  # Tell pdfkit where to find the css files to style the report
  css = [
    r'silvreport/static/report/style.css'
  ]

  html_template = get_template('report/fma-report.html')
  html = html_template.render(params)
  

  try:
    path_to_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
    config = pdfkit.configuration(wkhtmltopdf=path_to_wkhtmltopdf)
    pdf = pdfkit.from_string(html, False, css=css, options=options, configuration=config)
    response = HttpResponse(pdf, content_type = 'application/pdf')
    response['Content-Disposition'] = f'attachment; filename="{fma_name}.pdf"'
  except:
    logger.debug("The ROPA queries for the fma-report page succeded but wkhtmltopdf failed\
                  to render the PDF.")
  return response
  #return render(request, 'report/fma-report.html', params)
