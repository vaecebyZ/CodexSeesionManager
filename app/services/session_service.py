from app.models import SessionViewData, UserProfile


class SessionService:
    def load_view_data(self) -> SessionViewData:
        return SessionViewData(
            profile=UserProfile(),
            accounts=[],
        )
