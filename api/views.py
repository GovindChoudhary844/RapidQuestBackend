from django.http import JsonResponse
from django.views.decorators.http import require_GET
from .mongo_handler import MongoDBHandler
import logging

mongo_handler = MongoDBHandler()

# Configure logging
logger = logging.getLogger(__name__)


@require_GET
def total_sales_over_time(request, interval="daily"):
    try:
        data = mongo_handler.get_total_sales_over_time(interval=interval)
        return JsonResponse(data, safe=False)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@require_GET
def sales_growth_rate_over_time(request, interval="daily"):
    try:
        data = mongo_handler.get_sales_growth_rate(interval=interval)
        return JsonResponse(data, safe=False)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@require_GET
def new_customers_over_time(request, interval="daily"):
    try:
        data = mongo_handler.get_new_customers_over_time(interval=interval)
        return JsonResponse(data, safe=False)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@require_GET
def repeat_customers_over_time(request, interval="daily"):
    try:
        data = mongo_handler.get_repeat_customers_over_time(interval=interval)
        return JsonResponse(data, safe=False)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@require_GET
def geographical_distribution(request):
    try:
        data = mongo_handler.get_geographical_distribution()
        return JsonResponse(data, safe=False)
    except Exception as e:
        return JsonResponse({"error": str(e)}, status=500)


@require_GET
def customer_lifetime_value(request):
    try:
        data = mongo_handler.get_customer_lifetime_value()
        print(data)  # For debugging purposes
        return JsonResponse(data, safe=False)
    except Exception as e:
        print(f"Error: {str(e)}")  # For debugging purposes
        return JsonResponse({"error": str(e)}, status=500)
