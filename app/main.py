import os
import threading

import psycopg2
import uvicorn
from starlette.applications import Starlette
from starlette.responses import StreamingResponse


app = Starlette()

POSTGRES_URL = os.getenv('POSTGRES_URL')

assert POSTGRES_URL is not None and POSTGRES_URL != ''


def copy_to_pipe(fp_write):
    with psycopg2.connect(POSTGRES_URL) as conn:
        with conn.cursor() as cur:
            with os.fdopen(fp_write, 'w') as fp:
                fp.write('id,source_file,import_timestamp,source_file_hash,' +
                         'source_row_number,placeplace_near,day_of_week,' +
                         'date,time_of_day,accident_category,hit_and_run,' +
                         'urban,extra_urban,deaths,seriously_injured,' +
                         'slightly_injured,number_of_participants,' +
                         'pedestrian,bicycle,helmet,small_moped,moped,' +
                         'motorcycle,car,lorry,omnibus,other_road_user,' +
                         'participants_01,participants_01_registration,' +
                         'participants_02,participants_02_registration,' +
                         'alcoholized,accident_type,cause_1_4,cause_2,' +
                         'cause_3,cause_other,cause_02,light_conditions,' +
                         'road_condition,participants_child,' +
                         'participants_18_24,participants_senior,' +
                         'participants_age_01,participants_age_02\n')
                cur.copy_expert(
                    """
                    COPY (SELECT
                      id,
                      data->'import_timestamp',
                      data->'source_file_hash',
                      data->'source_row_number',
                      data->'place',
                      data->'place_near',
                      data->'day_of_week',
                      data->'date',
                      data->'time_of_day',
                      data->'accident_category',
                      data->'hit_and_run',
                      data->'urban',
                      data->'extra_urban',
                      data->'deaths',
                      data->'seriously_injured',
                      data->'slightly_injured',
                      data->'number_of_participants',
                      data->'pedestrian',
                      data->'bicycle',
                      data->'helmet',
                      data->'small_moped',
                      data->'moped',
                      data->'motorcycle',
                      data->'car',
                      data->'lorry',
                      data->'omnibus',
                      data->'other_road_user',
                      data->'participants_01',
                      data->'participants_01_registration',
                      data->'participants_02',
                      data->'participants_02_registration',
                      data->'alcoholized',
                      data->'accident_type',
                      data->'cause_1_4',
                      data->'cause_2',
                      data->'cause_3',
                      data->'cause_other',
                      data->'cause_02',
                      data->'light_conditions',
                      data->'road_condition',
                      data->'participants_child',
                      data->'participants_18_24',
                      data->'participants_senior',
                      data->'participants_age_01',
                      data->'participants_age_02'
                    FROM objects
                    WHERE
                      resource_name = 'record' AND
                      parent_id = '/buckets/accidents/collections/accidents_raw'
                    ) TO STDOUT
                    WITH DELIMITER ',' ENCODING 'utf8' NULL ''
                    """,
                    fp,
                )


@app.route("/")
async def homepage(request):
    fp_read, fp_write = os.pipe()
    thread = threading.Thread(target=copy_to_pipe, args=(fp_write,))
    thread.start()
    return StreamingResponse(os.fdopen(fp_read), media_type='text/csv')


if __name__ == "__main__":
    uvicorn.run(app, host='0.0.0.0', port=8080)
