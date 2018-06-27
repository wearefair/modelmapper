from unittest import mock
Column = mock.Mock()
Integer = mock.Mock()
String = mock.Mock()
DateTime = mock.Mock()
Boolean = mock.Mock()
Date = mock.Mock()
DECIMAL = mock.Mock()
SmallInteger = mock.Mock()


class SomeModel():
    __tablename__ = "some_model"

    # --------- THE FOLLOWING FIELDS ARE AUTOMATICALLY GENERATED. DO NOT CHANGE THEM OR REMOVE THIS LINE. some_model --------
    available = Column(Boolean, nullable=True)
    casualty = Column(Boolean, nullable=True)
    last_payment_date = Column(DateTime, nullable=True)
    make = Column(String(40), nullable=False, default='')
    score = Column(SmallInteger, nullable=True)
    slope = Column(DECIMAL(7, 6), nullable=True)
    value_current = Column(Integer, nullable=True)
    year = Column(String(4), nullable=False, default='')
    # --------- THE ABOVE FIELDS ARE AUTOMATICALLY GENERATED. DO NOT CHANGE THEM OR REMOVE THIS LINE. some_model --------
