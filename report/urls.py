from django.urls import path, reverse

from . import views

app_name = 'report'
urlpatterns= [
    path('', views.index, name='index'),
    path('timber-sale-report/', views.timber_sale_report, name='timber-sale-report'),
    path('fma-report/', views.fma_report, name='fma-report'),
    path('activity/', views.activity, name='activity')
]