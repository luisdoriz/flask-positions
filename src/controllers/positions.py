import numpy
from pymongo import MongoClient
import pandas as pd
from datetime import datetime
import requests
import os

from src.controllers.areas import AreaProcessor


class CoordsProcesor:
    def __init__(self, params):
        super(CoordsProcesor, self).__init__()
        self.url = f"{os.getenv('MAIN_API_URL')}/api/gateways"
        self.MONGO_DB = os.getenv("MONGO_DB")
        self.headers = {"Authorization": f"Bearer {os.getenv('MAIN_API_TOKEN')}"}
        self.timestamp = datetime.fromisoformat(params.get("timestamp"))
        self.gateways = []

    def fetch_gateways(self, mac_address):
        data = requests.get(
            self.url, json={"macAddress": mac_address}, headers=self.headers
        )
        data = data.json().get("data")
        beacons = [beacon.get("macAddress") for beacon in data.get("beacons")]
        gateways = dict()
        for gateway in data.get("gateways"):
            gateways[gateway.get("macAddress")] = {
                "x": gateway.get("x"),
                "y": gateway.get("y"),
            }
        return gateways, beacons

    def get_gateways(self, mac_address):
        if len(self.gateways) > 0:
            try:
                return next(
                    data.get("gateways")
                    for data in self.gateways
                    if mac_address in data.get("beacons")
                )
            except StopIteration as e:
                pass
        gateways, beacons = self.fetch_gateways(mac_address)
        self.gateways.append({"gateways": gateways, "beacons": beacons})
        return gateways

    def read_data(self):
        client = MongoClient(self.MONGO_DB)
        my_db = client["beacons"]
        df = pd.DataFrame(
            list(
                my_db["raw_beacons_data"]
                .find({"created_at": {"$gte": self.timestamp}})
                .sort("_id", -1)
            )
        )
        if len(df) == 0:
            return df
        df = df[pd.isna(df["meters"]) == False]
        df = df.sort_values(["mac_address", "created_at"])
        return df

    def insert_clean_positions(self, data):
        client = MongoClient(self.MONGO_DB)
        my_db = client["beacons"]
        my_db["beacons_data"].insert_many(data)
        return True

    def trilateration(self, a, b, c):
        a_positon = a.get("position")
        b_positon = b.get("position")
        c_positon = c.get("position")
        gateway1 = numpy.array(list(a_positon.values()))
        gateway2 = numpy.array(list(b_positon.values()))
        gateway3 = numpy.array(list(c_positon.values()))

        distA = a.get("meters")
        distB = b.get("meters")
        distC = c.get("meters")

        # Translate points so that A is at origin
        # C1 (0,0,0)
        C1 = gateway1 - gateway1
        gateway2a = gateway2 - gateway1
        gateway3a = gateway3 - gateway1

        # Rotate points so that B is at x-axis.
        # C2 (U, 0, 0)
        U = numpy.linalg.norm(gateway2a)  # Distance between origin (C1) and C2

        # Rotate C accordingly
        # C3 (Vx, Vy, 0)
        # gateway2a/U = unit vector of point 2a
        unitvector2 = gateway2a / U
        Vx = numpy.dot(unitvector2, gateway3a)  # X component of C3 vector
        distAC = numpy.linalg.norm(gateway3a)  # Distance between origin (C1) and C3
        # Vy = Pythagorean theorem with known hypothenuse (distAC) and a (Vx)
        Vy = numpy.sqrt((pow(distAC, 2) - pow(Vx, 2)))

        # apply formula with 3 known slants
        V = numpy.sqrt((pow(Vx, 2) + pow(Vy, 2)))
        rx = (pow(distA, 2) - pow(distB, 2) + pow(U, 2)) / (2 * U)
        ry = (pow(distA, 2) - pow(distC, 2) + pow(V, 2) - (2 * Vx * rx)) / (2 * Vy)

        # rerotate x,y
        # unit vector of
        unitvector3 = (gateway3 - gateway1 - Vx * unitvector2) / (
            numpy.linalg.norm(gateway3 - gateway1 - Vx * (gateway2a / U))
        )
        loc = gateway1 + unitvector2 * rx + ry * unitvector3  #
        loc[0] = round(loc[0], 2)
        loc[1] = round(loc[1], 2)
        return loc

    def process_beacon_data(self, beacon, data, gateways):
        created_ats = data["created_at"].astype(str).unique()
        outputs = list()
        errors = list()
        for created_at in created_ats:
            local = data[data["created_at"] == created_at]
            local_sorted = local.sort_values(["meters"])
            local_gateways = local_sorted.to_dict("records")
            print(local_gateways)
            print(gateways)
            gateways_data = [
                {"position": gateways[x.get("gateway")], "meters": x.get("meters")}
                for x in local_gateways
            ]
            if len(gateways_data) < 3:
                errors.append(local)
                continue
            a, b, c = gateways_data
            x, y = self.trilateration(a, b, c)

            output = {
                "x": str(x),
                "y": str(y),
                "created_at": datetime.fromisoformat(created_at),
                "beacon": beacon,
            }
            outputs.append(output)
        return outputs

    def process_data(self, data):
        beacons = data["mac_address"].unique()
        amount = 0
        for beacon in beacons:
            gateways = self.get_gateways(beacon)
            beacon_data = data[data["mac_address"] == beacon]
            beacon_output = self.process_beacon_data(beacon, beacon_data, gateways)
            amount += len(beacon_output)
            self.insert_clean_positions(beacon_output)
        print(f"total of {amount} positions saved")

    def main(self):
        df = self.read_data()
        df["meters"] = df["meters"].round(6)
        self.process_data(df)
        area_processor = AreaProcessor({"timestamp": str(self.timestamp)})
        area_processor.main()
