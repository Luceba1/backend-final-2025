"""
Centralized Enums Module

Contains all shared enumeration types used across models and schemas.
"""
from enum import Enum


class DeliveryMethod(Enum):
    """Order delivery method options"""
    DRIVE_THRU = 1
    ON_HAND = 2
    HOME_DELIVERY = 3


class Status(Enum):
    """Order status options"""
    PENDING = 1
    IN_PROGRESS = 2
    DELIVERED = 3
    CANCELED = 4


class PaymentType(Enum):
    """Bill payment type options"""
    CASH = 1
    CARD = 2
    DEBIT = 3
    CREDIT = 4
    BANK_TRANSFER = 5
