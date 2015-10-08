function (doc, meta) {
    if (meta.type == "json") {
      if(doc.doc_type && doc.doc_type == "Location") {
        if (doc.user_id) {
          emit(
          [
            {
               "type": "Point",
               "coordinates": [doc.latitude, doc.longitude]
            },
            [doc.time_start, doc.time_end],
          ],
          {
            object_id: meta.id,
          });
        }
      }
    }
}