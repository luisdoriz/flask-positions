from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import requests
import pandas as pd
import os

MAIN_API_URL = os.getenv("MAIN_API_URL")
MONGO_DB = os.getenv("MONGO_DB")


class AreaProcessor:
    def __init__(self):
        super(AreaProcessor, self).__init__()
        self.INT_MAX = 10000
        self.beacons_data = self.get_beacons_data()
        self.areas = {}

    def get_orientation(self, p1, p2, p3):

        # difference in slopes between points p1-p2 and p2-p3
        # if the first slope is higher, orientation is clockwise
        # if the second slope is higher, orientation is counterclockwise
        # if both slopes are equal, orientation is collinear
        dif = ((p2[1] - p1[1]) * (p3[0] - p2[0])) - ((p2[0] - p1[0]) * (p3[1] - p2[1]))

        if dif == 0:
            return 0  # Collinear
        elif dif > 0:
            return 1  # Clockwise
        else:
            return 2  # Counterclockwise

    def is_on_segment(self, p1, point, p2):
        # Check if point is in segment p1p2
        if (
            (point[0] <= max(p1[0], p2[0]))
            & (point[0] >= min(p1[0], p2[0]))
            & (point[1] <= max(p1[1], p2[1]))
            & (point[1] >= min(p1[1], p2[1]))
        ):
            return True

        return False

    def get_intersect(self, p1, p2, p3, inf):

        # Find the four orientations needed for
        # general and special cases
        o1 = self.get_orientation(
            p1, p2, p3
        )  # checks if p3 (point we're looking for) is right or left of segment p1p2
        o2 = self.get_orientation(
            p1, p2, inf
        )  # checks if inf is right or left of segment p1p2
        o3 = self.get_orientation(
            p3, inf, p1
        )  # checks if p1 (part of segment p1p2) is up or down of segment p3inf
        o4 = self.get_orientation(
            p3, inf, p2
        )  # checks if p2 (part of segment p1p2) is up or down of segment p3inf

        # General case
        # if o1 and o2 are different, it means that the p1p2 segment is between p3inf horizontally
        # if o3 and o4 are different, it means that the p1p2 segment is between p3inf vertically
        if o1 != o2 and o3 != o4:
            return True

        # Special Cases
        # p1, p2 and p3 are colinear and
        # p3 lies on segment p1p2
        if (o1 == 0) and (self.is_on_segment(p1, p3, p2)):
            return True

        # p1, p2 and p3 are colinear and
        # inf lies on segment p1p2
        if (o2 == 0) and (self.is_on_segment(p1, inf, p2)):
            return True

        # p3, inf and p1 are colinear and
        # p1 lies on segment p3inf
        if (o3 == 0) and (self.is_on_segment(p3, p1, inf)):
            return True

        # p3, inf and p2 are colinear and
        # p2 lies on segment p3inf
        if (o4 == 0) and (self.is_on_segment(p3, p2, inf)):
            return True

        return False

    def is_inside_area(self, area, point):
        n = len(area)
        if n < 3:
            return False

        # infinte collinear with point
        extreme = (self.INT_MAX, point[1])
        count = i = 0

        while True:
            next = (i + 1) % n

            # Check if the line segment from 'p' to
            # 'extreme' intersects with the line
            # segment from 'polygon[i]' to 'polygon[next]'
            if self.get_intersect(area[i], area[next], point, extreme):
                # If the point 'p' is colinear with line
                # segment 'i-next', then check if it lies
                # on segment. If it lies, return true, otherwise false
                if self.get_orientation(area[i], point, area[next]) == 0:
                    return self.is_on_segment(area[i], point, area[next])

                count += 1

            i = next

            if i == 0:
                break
        # Return true if count is odd, false otherwise
        return count % 2 == 1

    def get_beacon_facility(self, mac_address):
        areas = self.areas.get(mac_address)
        if areas is None:
            url = f"{MAIN_API_URL}/api/areas/beacon"
            r = requests.get(url, json={"macAddress": mac_address}).json().get("data")
            self.areas[mac_address] = r
            areas = r
        return areas

    def process_beacons_data(self, beacon_data):
        point = [beacon_data.get("x"), beacon_data.get("y")]
        mac_address = beacon_data.get("beacon")
        areas = self.get_beacon_facility(mac_address)
        area_id = next(
            area.get("idArea")
            for area in areas
            if self.is_inside_area(area.get("vertices"), point)
        )
        output = {
            "beacon": mac_address,
            "area": area_id,
            "x": point[0],
            "y": point[1],
            "created_at": beacon_data.get("created_at"),
        }
        return output

    def get_beacons_data(self):
        client = MongoClient(MONGO_DB)
        my_db = client["beacons"]
        output = list(my_db["beacons_data"].find())
        return output

    def clean_by_rows(self, data):
        current_timestamp = None
        first_timestamp = None
        local_rows = []
        coords = []
        new_rows = []
        for row in data:
            timestamp = row.get("created_at").timestamp()
            if current_timestamp is None:
                current_timestamp = timestamp
                first_timestamp = timestamp
                coords = [row.get("x"), row.get("y")]
                continue
            seconds = timestamp - current_timestamp
            x, y = coords
            row_x, row_y = [row.get("x"), row.get("y")]
            if seconds > 5 and row_x != x and row_y != y:
                sum_of_time = current_timestamp - first_timestamp
                start = datetime.fromtimestamp(first_timestamp)
                end = datetime.fromtimestamp(current_timestamp)
                output = {
                    "from": start,
                    "to": end,
                    "time_spent": sum_of_time,
                    "created_at": row.get("created_at"),
                    "beacon": row.get("beacon"),
                    "area": row.get("area"),
                    "x": row.get("x"),
                    "y": row.get("y"),
                }
                new_rows.append(output)
                first_timestamp = timestamp
                coords = [row.get("x"), row.get("y")]
            current_timestamp = timestamp
        return new_rows

    def clean_by_areas(self, data):
        areas = list(data.area.unique())
        rows = []
        for area in areas:
            area_data = data[data["area"] == area].to_dict("records")
            new_rows = self.clean_by_rows(area_data)
            rows.extend(new_rows)
        return rows

    def clean_by_beacons(self, data):
        beacons = list(data.beacon.unique())
        rows = []
        for beacon in beacons:
            beacon_data = data[data["beacon"] == beacon]
            new_rows = self.clean_by_areas(beacon_data)
            rows.extend(new_rows)
        return rows

    def clean_data(self, data):
        df = pd.DataFrame(data)
        df = df.sort_values("created_at")
        df["created_at"] = pd.to_datetime(df["created_at"])
        df = df.round()
        return self.clean_by_beacons(df)

    def save_positions(self, data):
        body = {"positions": data}
        requests.put("{MAIN_API_URL}/api/positions", body)

    def main(self):
        rows = []
        for beacon in self.beacons_data:
            try:
                row = self.process_beacons_data(beacon)
                rows.append(row)
            except:
                continue
        data = self.clean_data(rows)
        print(data)
