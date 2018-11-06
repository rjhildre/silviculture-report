
import os
from django.template.loader import get_template, render_to_string
from django.http import HttpResponse 
from django.shortcuts import render, get_object_or_404
from django.template import Context
import cx_Oracle
import pdfkit
import subprocess


# Oracle database connection configuration
ORACLE_DATABASE = os.environ.get('ROPA')
ORACLE_USERNAME = 'gis_layer_user'
ORACLE_PASSWORD = 'gis_layer_user_ropa'

def index(request):
  # Query ropa to get a list of timbersale names and id's to populate the selection table of the index page. 
  connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, ORACLE_DATABASE)
  cursor = connection.cursor()
  cursor.execute("select TS_ID, TS_NM from SHARED_LRM.TS order by TS_NM")
  timber_sales = cursor.fetchall()
  cursor.close()
  connection.close()
  return render(request, 'report/index.html', {'timber_sales' : timber_sales})

def fma_report(request):
  timber_sale=request.GET.get('timber-sale')
  timber_sale = timber_sale.split(' -')[0]
  # Create a list of FMAs, and their details, that overlap the timber harvest FMAs
  fma_overlap_query = f"""
  SELECT
    SHARED_LRM.FMA_V.FMA_ID,
    SHARED_LRM.FMA_V_1.FMA_ID,
    FMA_V_1.FMA_NM,
    FMA_V_1.FMA_TYPE_CD,
    FMA_V_1.TECHNIQUE_CD,
    FMA_V_1.FMA_DT,
    FMA_V_1.FMA_STATUS_CD,
    SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP,
    FMA_V_1.COMMENTS
  FROM
    SHARED_LRM.TS
    INNER JOIN SHARED_LRM.FMA_V
    INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
    INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
    ON SHARED_LRM.TS.TS_ID = SHARED_LRM.FMA_V.TS_ID
  WHERE
    SHARED_LRM.TS.TS_NM = '{timber_sale}' AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.1
  ORDER BY
    SHARED_LRM.FMA_V_1.FMA_DT
  """
  
  connection = cx_Oracle.connect(ORACLE_USERNAME, ORACLE_PASSWORD, ORACLE_DATABASE)
  cursor = connection.cursor()
  cursor.execute(fma_overlap_query)
  # overlapping_fmas = list(row for row in cursor.fetchall())
  overlapping_fmas = []
  for row in cursor.fetchall():
    # The comments field is a LOB object, so we need the read method, which won't work if value is None. 
    if row[8] != None:
      comments = row[8].read()
    else:
      comments = "None"
    
    overlapping_fmas.append([row[0], row[1], row[2], row[3], row[4], row[5], row[6], row[7], comments])
  
  

  # Get the info for overlapping fmas that have regen activity (i.e. they hit the REGEN table)
  regen_query = f"""
  SELECT
    FMA_V_1.FMA_ID,
    SHARED_LRM.FMA_REGEN.SPECIES_CD,
    SHARED_LRM.FMA_REGEN.STOCK_TYPE_CD,
    SHARED_LRM.FMA_REGEN.TPA,
    SHARED_LRM.FMA_REGEN.AREA_REGEN,
    SHARED_LRM.FMA_REGEN.STOCK_TOTAL
  FROM
    SHARED_LRM.FMA_V
    INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
    INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
    INNER JOIN SHARED_LRM.FMA_REGEN  ON FMA_V_1.FMA_ID = SHARED_LRM.FMA_REGEN.FMA_ID
    INNER JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_V.TS_ID = SHARED_LRM.TS.TS_ID
  WHERE
    SHARED_LRM.TS.TS_NM = '{timber_sale}' AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.1
  """
  cursor.execute(regen_query)
  regen = [row for row in cursor.fetchall()]

  #Get the info for the overlapping fmas that have managment activities (i.e. they hit the MATERIAL table)
  material_query = f"""
  SELECT
    FMA_V_1.FMA_ID,
    SHARED_LRM.FMA_MATERIAL.MATERIAL_QTY,
    SHARED_LRM.FMA_MATERIAL.UOM_CD,
    SHARED_LRM.FMA_MATERIAL.MATERIAL_QTY_AC,
    SHARED_LRM.FMA_MATERIAL.MATERIAL_COST,
    SHARED_LRM.FMA_MATERIAL.MATERIAL_COST_AC
  FROM
    SHARED_LRM.FMA_V
    INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
    INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
    INNER JOIN SHARED_LRM.FMA_MATERIAL  ON FMA_V_1.FMA_ID = SHARED_LRM.FMA_MATERIAL.FMA_ID
    INNER JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_V.TS_ID = SHARED_LRM.TS.TS_ID
  WHERE
    SHARED_LRM.TS.TS_NM = '{timber_sale}' AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.1
  """
  cursor.execute(material_query)
  material = [row for row in cursor.fetchall()]

  # Get the info for the overlapping fmas that have survey activities (i.e. they hit the SURVEY table)
  survey_query = f"""
  SELECT
    FMA_V_1.FMA_ID,
    SHARED_LRM.FMA_SURVEY.SPECIES_CD,
    SHARED_LRM.FMA_SURVEY.TPA_ALL,
    SHARED_LRM.FMA_SURVEY.TPA_PLANTED,
    SHARED_LRM.FMA_SURVEY.TPA_NATURAL,
    SHARED_LRM.FMA_SURVEY.DAMAGE_CD,
    SHARED_LRM.FMA_SURVEY.DBH,
    SHARED_LRM.FMA_SURVEY.CROWN_RATIO,
    SHARED_LRM.FMA_SURVEY.SURVIVAL_PERCENT,
    SHARED_LRM.FMA_SURVEY.HEIGHT

  FROM
    SHARED_LRM.FMA_V
    INNER JOIN SHARED_LRM.XREF_OVL_FMA_OVERLAP ON SHARED_LRM.FMA_V.FMA_ID = SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_ID
    INNER JOIN SHARED_LRM.FMA_V FMA_V_1 ON SHARED_LRM.XREF_OVL_FMA_OVERLAP.FMA_XREF_ID = FMA_V_1.FMA_ID
    INNER JOIN SHARED_LRM.FMA_SURVEY  ON FMA_V_1.FMA_ID = SHARED_LRM.FMA_SURVEY.FMA_ID
    INNER JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_V.TS_ID = SHARED_LRM.TS.TS_ID
  WHERE
    SHARED_LRM.TS.TS_NM = '{timber_sale}' AND SHARED_LRM.XREF_OVL_FMA_OVERLAP.ACRES_OVERLAP > 0.1
  """
  cursor.execute(survey_query)
  survey = [row for row in cursor.fetchall()]

  # Get the details for each fma in the timber sale
  fma_detail_query = f"""
  SELECT
  SHARED_LRM.FMA_HARVEST.FMA_ID,
  SHARED_LRM.TS.TS_NM,
  SHARED_LRM.FMA_HARVEST.COMMENTS,
  SHARED_LRM.FMA_V.FMA_NM,
  SHARED_LRM.FMA_V.FMA_STATUS_CD,
  SHARED_LRM.FMA_V.FMA_DT,
  SHARED_LRM.FMA_V.FMA_TYPE_CD,
  SHARED_LRM.FMA_V.TECHNIQUE_CD,
  SHARED_LRM.FMA_V.CREW_CD,
  SHARED_LRM.FMA_V.ACRES_TREATED,
  SHARED_LRM.FMA_V.COMMENTS,
  SHARED_LRM.FMA_V.REGION_NM,
  SHARED_LRM.FMA_V.DISTRICT_NM,
  SHARED_LRM.FMA_V.ADMIN_NM
  FROM
      SHARED_LRM.FMA_HARVEST
      RIGHT JOIN SHARED_LRM.TS ON SHARED_LRM.FMA_HARVEST.TS_ID = SHARED_LRM.TS.TS_ID
      LEFT JOIN SHARED_LRM.FMA_V ON SHARED_LRM.FMA_HARVEST.FMA_ID = SHARED_LRM.FMA_V.FMA_ID
  WHERE
    SHARED_LRM.TS.TS_NM = '{timber_sale}'
  ORDER BY
    SHARED_LRM.FMA_V.FMA_NM
  """
  cursor.execute(fma_detail_query)
  fma_details = {}
  # Deal with the LOB object again
  for row in cursor.fetchall():
    if row[10] != None:
      fma_comments = row[10].read()
    else:
      fma_comments = "None"
    fma_details[row[3]] = {}
    fma_details[row[3]]["fma_id"] = row[0]
    fma_details[row[3]]["harvest_comments"] = row[2]
    fma_details[row[3]]["fma_status"] = row[4]
    fma_details[row[3]]["fma_date"] = row[5]
    fma_details[row[3]]["fma_type"] = row[6]
    fma_details[row[3]]["fma_technique"] = row[7]
    fma_details[row[3]]["fma_crew"] = row[8]
    fma_details[row[3]]["fma_acres_treated"] = row[9]
    fma_details[row[3]]["fma_comments"] = fma_comments
    fma_details[row[3]]["fma_region"] = row[11]
    fma_details[row[3]]["fma_district"] = row[12]
    fma_details[row[3]]["fma_admin"] = row[13]
  cursor.close()
  connection.close()
  del cursor
  del connection
  params = {
    'overlapping_fmas' : overlapping_fmas,
    'fma_details' : fma_details,
    'timber_sale' : timber_sale,
    'regen' : regen,
    'material' : material,
    'survey' : survey
  }
  options = {
    'print-media-type' : '',
    'page-size' : 'Letter',
    'footer-center':'[page] of [topage]'
  }
  css = [
    'report/static/report/bootstrap/css/bootstrap.min.css',
    'report/static/report/style.css'
  ]

  html_template = get_template('report/fma-report.html')
  html = html_template.render(params)
  


  path_to_wkhtmltopdf = r'C:\Program Files\wkhtmltopdf\bin\wkhtmltopdf.exe'
  config = pdfkit.configuration(wkhtmltopdf=path_to_wkhtmltopdf)
  pdf_options = {
    
  }
  pdf = pdfkit.from_string(html, False, css=css, options=options, configuration=config)
  response = HttpResponse(pdf, content_type = 'application/pdf')
  response['Content-Disposition'] = 'attachment; filename="report.pdf"'
  return response
  #return render(request, 'report/fma-report.html', params)

