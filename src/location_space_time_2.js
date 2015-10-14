function (doc, meta) {
    if (meta.type == "json") {
      if(doc.docType && doc.docType == "Location") {
        if (doc.userId) {
          emit(
          [
            [doc.timeStart, doc.timeEnd],
            [doc.latitude, doc.latitude],
            [doc.longitude, doc.longitude]
          ],
          {
            objectId: meta.id,
          });
        }
      }
    }
}