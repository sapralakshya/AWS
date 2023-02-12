import json
import random
car_name_list=['Celerio','Swift','Altroz','Alto','Verna','Ciaz','Scorpio','Creta','Brezza','Harrier']
car_type_list=['hatch-back','hatch-back','hatch-back','hatch-back','sedan','sedan','SUV','SUV','SUV','SUV']
car_model_list=['2018','2016','2020','2015','2017','2016','2022','2019','2021','2020']
car_price_list=['4lacs','4.5lacs','6lacs','2lacs','6.5lacs','6lacs','11lacs','10lacs','9lacs','11.5lacs']

def lambda_handler(event, context):
    # TODO implement
    random_index=random.randint(0,9)
    return {
        "car_name" : car_name_list[random_index],
        "car_type" : car_type_list[random_index],
        "car_model" : car_model_list[random_index],
        "car_price" : car_price_list[random_index]
    }
