import datetime
import os
import logging

from sqlalchemy import (
    create_engine,
    Column,
    Integer,
    String,
    Float,
    Boolean,
    Date,
    Time,
    DateTime,
)
from sqlalchemy.orm import declarative_base, sessionmaker
from dotenv import load_dotenv

from algo_scripts.algotrade.scripts.trade_utils.time_manager import (
    get_current_ist_time_as_str,
    get_today_date_as_str,
    get_screener_run_id,
    get_ist_time,
)

from algo_scripts.algotrade.scripts.trading_style.intraday.core.intra_utils.db.management.database_manager import engine, get_db_session, Base

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger("SG_ORB_SCREENER")



class SgOrbAlerts(Base):
    __tablename__ = "sg_orb_screener"

    id = Column(Integer, primary_key=True, autoincrement=True)
    run_id = Column(String(100))
    screener_date = Column(Date)                 # orb_date
    screener_time = Column(Time)                 # derived from orb_time
    trade_type = Column(String(100))             # "INTRADAY"
    stock_type = Column(String(100))             # "CASH"
    strategy = Column(String(100))               # ORB+PRB 15 / ORB 30 etc.
    updated_time = Column(DateTime)              # insert timestamp
    symbol = Column(String(100))
    ltp = Column(Float)
    orb_price = Column(Float)
    deviation = Column(String(100))              # from scraper
    range = Column(String(100))                  # from scraper
    time_range_orb = Column(String(100))         # "15" / "30" / etc.
    is_prb_present = Column(Boolean)             # True/False
    orb_time = Column(Time)                      # parsed from string
    last_updated = Column(DateTime, nullable=True)
    change = Column(String(100))

    def __repr__(self):
        return f"<SgOrbAlerts(symbol='{self.symbol}', strategy='{self.strategy}', orb_time='{self.orb_time}')>"


class SgOrbRepository:
    def __init__(self, session):
        self.engine = engine
        self.Session = sessionmaker(bind=self.engine)

    def insert(self, data: dict):
        session = self.Session()
        try:
            # Parse date + time from scraper fields
            current_date = get_ist_time()[1].date()


            orb_time_val = None
            if data.get("orb_time"):
                try:
                    orb_time_val = datetime.datetime.strptime(
                        data["orb_time"], "%I:%M %p"
                    ).time()
                except ValueError:
                    pass

            alert = SgOrbAlerts(
                run_id=data.get("run_id", get_screener_run_id()),
                screener_date=current_date,
                screener_time=orb_time_val,
                trade_type=data.get("trade_type", "INTRADAY"),
                stock_type=data.get("stock_type", "CASH"),
                strategy=data.get("strategy"),
                updated_time=datetime.datetime.now(),
                symbol=data.get("symbol"),
                ltp=float(data["ltp"].split('\n')[0]),
                orb_price=float(data["orb_price"]) if data.get("orb_price") else None,
                deviation=data.get("deviation"),
                range=data.get("range"),
                time_range_orb=str(data.get("time_range_orb")),
                is_prb_present=data.get("is_prb_present", False),
                orb_time=orb_time_val,
                last_updated=data.get("last_updated"),
                change=','.join(data.get("ltp").split('\n')[1:])
            )
            session.add(alert)
            session.commit()
            logger.info(f"Inserted alert for {alert.symbol}")
        except Exception as e:
            session.rollback()
            logger.error(f"Failed to insert data: {e}")
        finally:
            session.close()


if __name__ == "__main__":
    Base.metadata.create_all(engine)
    repo = SgOrbRepository(get_db_session)
