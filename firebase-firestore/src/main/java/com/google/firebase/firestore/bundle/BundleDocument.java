// Copyright 2021 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//      http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package com.google.firebase.firestore.bundle;

import com.google.firebase.firestore.model.Document;
import com.google.firebase.firestore.model.DocumentKey;

public class BundleDocument extends BundleElement {
  private DocumentKey key;
  private Document document;

  public BundleDocument(DocumentKey key, Document document) {
    super();
    this.key = key;
    this.document = document;
  }

  public DocumentKey getKey() {
    return key;
  }

  public Document getDocument() {
    return document;
  }
}
