function (doc, meta) {
    if (meta.type == "json") {
      if(doc.docType && doc.docType == "Location") {
        if (doc.userId) {
          emit(
          [
            doc.userId,
            doc.timeStart
          ], null);
        }
      }
    }
}
