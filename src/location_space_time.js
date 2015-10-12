function (doc, meta) {
    if (meta.type == "json") {
      if(doc.docType && doc.docType == "Location") {
        if (doc.userId) {
          emit(
          [
            {
               "type": "Point",
               "coordinates": [doc.latitude, doc.longitude]
            },
            [doc.timeStart, doc.timeEnd],
          ],
          {
            objectId: meta.id,
          });
        }
      }
    }
}