from django.urls import path

from . import views

app_name = 'report'
urlpatterns= [
    path('', views.index, name='index'),
    path('timber-sale-report/', views.timber_sale_report, name='timber-sale-report'),
    path('fma-report/', views.fma_report, name='fma-report')
]