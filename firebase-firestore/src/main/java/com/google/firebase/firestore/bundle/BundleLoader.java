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

import static com.google.firebase.firestore.model.DocumentCollections.emptyMaybeDocumentMap;
import static com.google.firebase.firestore.util.Assert.hardAssert;

import androidx.annotation.Nullable;
import com.google.firebase.database.collection.ImmutableSortedMap;
import com.google.firebase.database.collection.ImmutableSortedSet;
import com.google.firebase.firestore.LoadBundleTaskProgress;
import com.google.firebase.firestore.local.LocalStore;
import com.google.firebase.firestore.model.DocumentKey;
import com.google.firebase.firestore.model.MaybeDocument;
import com.google.firebase.firestore.model.NoDocument;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class BundleLoader {
  private final LocalStore localStore;
  private final BundleMetadata bundleMetadata;
  private int documentsLoaded;
  private long bytesLoaded;
  private final List<NamedQuery> queries;
  private ImmutableSortedMap<DocumentKey, MaybeDocument> documents;
  private final Map<DocumentKey, BundledDocumentMetadata> documentsMetadata;
  @Nullable private DocumentKey currentDocument;

  public BundleLoader(LocalStore localStore, BundleMetadata bundleMetadata) {
    this.localStore = localStore;
    this.bundleMetadata = bundleMetadata;
    this.queries = new ArrayList<>();
    this.documents = emptyMaybeDocumentMap();
    this.documentsMetadata = new HashMap<>();
  }

  /**
   * Adds an element from the bundle to the loader.
   *
   * <p>Returns a new progress if adding the element leads to a new progress, otherwise returns
   * null.
   */
  public @Nullable LoadBundleTaskProgress addElement(BundleElement bundleElement, long byteSize) {
    hardAssert(!(bundleElement instanceof BundleMetadata), "Unexpected bundle metadata  element.");

    bytesLoaded += byteSize;

    int documentsLoaded = this.documentsLoaded;

    if (bundleElement instanceof NamedQuery) {
      queries.add((NamedQuery) bundleElement);
    } else if (bundleElement instanceof BundledDocumentMetadata) {
      BundledDocumentMetadata bundledDocumentMetadata = (BundledDocumentMetadata) bundleElement;
      documentsMetadata.put(bundledDocumentMetadata.getKey(), bundledDocumentMetadata);
      currentDocument = bundledDocumentMetadata.getKey();
      if (!((BundledDocumentMetadata) bundleElement).exists()) {
        documents =
            documents.insert(
                bundledDocumentMetadata.getKey(),
                new NoDocument(
                    bundledDocumentMetadata.getKey(),
                    bundledDocumentMetadata.getReadTime(),
                    /* hasCommittedMutations= */ false));
        ++documentsLoaded;
      }
    } else if (bundleElement instanceof BundleDocument) {
      BundleDocument bundleDocument = (BundleDocument) bundleElement;
      if (!bundleDocument.getKey().equals(currentDocument)) {
        throw new IllegalArgumentException(
            "The document being added does not match the stored metadata.");
      }
      documents = documents.insert(bundleDocument.getKey(), bundleDocument.getDocument());
      currentDocument = null;
      ++documentsLoaded;
    }

    if (documentsLoaded != this.documentsLoaded) {
      this.documentsLoaded = documentsLoaded;
      return new LoadBundleTaskProgress(
          documentsLoaded,
          bundleMetadata.getTotalDocuments(),
          bytesLoaded,
          bundleMetadata.getTotalBytes(),
          null,
          LoadBundleTaskProgress.TaskState.RUNNING);
    }

    return null;
  }

  /** Update the progress to 'Success' and return the updated progress. */
  public LoadBundleTaskProgress applyChanges() {
    if (currentDocument != null) {
      throw new IllegalArgumentException(
          "Bundled documents end with a document metadata element instead of a document.");
    }
    if (bundleMetadata.getBundleId() == null) {
      throw new IllegalArgumentException("Bundle ID must be set");
    }

    localStore.applyBundledDocuments(documents, bundleMetadata.getBundleId());
    Map<String, ImmutableSortedSet<DocumentKey>> queryDocumentMap = getQueryDocumentMapping();

    for (NamedQuery namedQuery : queries) {
      localStore.saveNamedQuery(namedQuery, queryDocumentMap.get(namedQuery.getName()));
    }

    return new LoadBundleTaskProgress(
        documentsLoaded,
        bundleMetadata.getTotalDocuments(),
        bytesLoaded,
        bundleMetadata.getTotalBytes(),
        null,
        LoadBundleTaskProgress.TaskState.SUCCESS);
  }

  public ImmutableSortedMap<DocumentKey, MaybeDocument> getChangedDocuments() {
    return documents;
  }

  private Map<String, ImmutableSortedSet<DocumentKey>> getQueryDocumentMapping() {
    Map<String, ImmutableSortedSet<DocumentKey>> queryDocumentMap = new HashMap<>();
    for (BundledDocumentMetadata metadata : documentsMetadata.values()) {
      for (String query : metadata.getQueries()) {
        ImmutableSortedSet<DocumentKey> matchingKeys = queryDocumentMap.get(query);
        if (matchingKeys == null) {
          matchingKeys = DocumentKey.emptyKeySet();
        }
        queryDocumentMap.put(query, matchingKeys.insert(metadata.getKey()));
      }
    }

    return queryDocumentMap;
  }
}
