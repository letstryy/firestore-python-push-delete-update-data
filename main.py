import os
import json
import asyncio
import json
import firebase_admin
from firebase_admin import credentials, firestore
from google.cloud import storage

class FirestorePush:

    def __init__(self, bucketName, filename):

        try:
             self.db_admin = firebase_admin.initialize_app()
        except:
             self.db_admin = firebase_admin.get_app()

        self.db = firestore.client()
        self.fl = filename

        if self.fl.split('.')[0] == 'a':
            self.doc_id =  'ID'

        storage_client = storage.Client()
        bucket = storage_client.get_bucket(bucketName)
        blob = bucket.blob(filename)
        self.records_db = json.loads(blob.download_as_string(client=None))

    async def delete_batch(self, coll_ref, batch_size, counter):

        docs = coll_ref.limit(batch_size).stream()
        counter = await self.update_collection(coll_ref, batch_size, docs)
        if counter >= batch_size:
            return await self.create_batch(coll_ref, batch_size, counter)

    async def update_batch(self, coll_ref):

        await self.update_collection(coll_ref)

    async def delete_collection(self, coll_ref, batch_size, docs):

        print('Deleting Collection!')
        batch = self.db.batch()
        counter = 0

        for doc in docs:
            counter = counter + 1
            if counter % 500 == 0:
                await self.commit_batch(batch)
            batch.delete(doc.reference)

        await self.commit_batch(batch)
        return counter

    async def commit_batch(self, batch):

        print('Committing..')
        batch.commit()

    async def push(self):

        pushes = 0
        fl = self.fl.split('.')[0]
        print('Pushing Records:', fl)
        records_collection = self.db.collection(str(fl))

        total = len(self.records_db)
        idx = 0

        batch = self.db.batch()
        if fl == 'car_scores':
            for k,v in self.records_db.items():
                if idx % 500 == 0:
                    if idx > 0:
                        await self.commit_batch(batch)

                    batch = self.db.batch()
                idx += 1
                doc_ref = records_collection.document(k)
                pushes = pushes + 1
                batch.set(doc_ref, v)
        else:
            for record in self.records_db:
                if idx % 500 == 0:
                    if idx > 0:
                        await self.commit_batch(batch)

                    batch = self.db.batch()
                idx += 1
                doc_ref = records_collection.document(str(record[self.doc_id]))
                pushes = pushes + 1
                batch.set(doc_ref, record)

        print('Number of Pushed: ', pushes)
        if idx % 500 != 0:
            await self.commit_batch(batch)

    async def update_collection(self, coll_ref):

        print('Updating Collection!')
        batch = self.db.batch()
        counter = 0
        update = 0
        t = {}
        l = []

        if self.fl.split('.')[0] == 'a':
            for record in self.records_db:
                if counter % 500 == 0:
                    await self.commit_batch(batch)

                docs = coll_ref.document(record['ID']).get()
                dt = docs.to_dict()

                if dt:
                    counter = counter + 1
                    update = update + 1
                    batch.update(docs.reference, record)
                    l.append(record['ID'])
                else:
                    print('New Record!')

            self.records_db = [d for d in self.records_db if d['ID'] not in l]

        else:
            for k,v in self.records_db.items():
                if counter % 500 == 0:
                    await self.commit_batch(batch)

                docs = coll_ref.document(k).get()
                dt = docs.to_dict()

                if dt:
                    counter = counter + 1
                    update = update + 1
                    batch.update(docs.reference, v)
                    t.update({k:v})
                else:
                    print('New Record!')

            for k,v in t.items():
                del self.records_db[k]

        print('Number of Updates: ', update)
        await self.commit_batch(batch)
        return counter

    async def final(self):

        fn = self.fl.split('.')[0]
        await self.update_batch(self.db.collection(fn))
        await self.push()
        print('Done!')

def get_gcs(event, context):

    file = event
    print(f"Processing file: {file['name']}.")
    bucketName = event['bucket']
    filename = event['name']
    fn = filename.split('.')[0]
    print(fn)
    ## dict of dict and list of dicts
    names = ['b', 'a']
    if fn in names:
        f = FirestorePush(bucketName, filename)
        asyncio.run(f.final())
    else:
        print("Error: Wrong Collection name!")
