from src.data_loader.data_loader import DataLoader
from src.db.mediator_db import db


class WCSLoader(DataLoader):
    @staticmethod
    def validate(url):
        return "/wcs?" in url

    def load(self):
        # Load data to self.table_name
        db.save_fake_data(self.table_name)

        # Update the status
        db.update_data_status(self.url, 'Saved')
