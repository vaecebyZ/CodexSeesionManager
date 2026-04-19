from dataclasses import dataclass
from typing import List


@dataclass
class UserProfile:
    user_id: str = "-"
    user_name: str = "-"
    user_email: str = "-"
    expire_time: str = "-"


@dataclass
class AccountToken:
    account_id: str
    plan_type: str
    structure: str
    access_token: str
    session_token: str


@dataclass
class SessionViewData:
    profile: UserProfile
    accounts: List[AccountToken]


@dataclass
class SessionFetchResult:
    view_data: SessionViewData
    message: str = ""
