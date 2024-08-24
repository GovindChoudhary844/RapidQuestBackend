from django.urls import path
from . import views

urlpatterns = [
    path('total-sales/', views.total_sales_over_time, name='total_sales_over_time'),
    path('sales-growth-rate/', views.sales_growth_rate_over_time, name='sales_growth_rate_over_time'),
    path('new-customers/', views.new_customers_over_time, name='new_customers_over_time'),
    path('repeat-customers/', views.repeat_customers_over_time, name='repeat_customers_over_time'),
    path('geographical-distribution/', views.geographical_distribution, name='geographical_distribution'),
    path('customer-lifetime-value/', views.customer_lifetime_value, name='customer_lifetime_value'),
]
