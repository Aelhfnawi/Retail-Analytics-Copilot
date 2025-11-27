import os
import glob
from typing import List, Dict
from sklearn.feature_extraction.text import TfidfVectorizer
from sklearn.metrics.pairwise import cosine_similarity
import numpy as np

class Retriever:
    def __init__(self, docs_dir: str = "docs"):
        self.docs_dir = docs_dir
        self.chunks = []
        self.vectorizer = None
        self.tfidf_matrix = None
        self._load_and_chunk_docs()
        self._build_index()

    def _load_and_chunk_docs(self):
        """
        Loads markdown files from docs_dir and chunks them by headers or paragraphs.
        Simple chunking strategy: Split by '## ' or double newlines.
        """
        md_files = glob.glob(os.path.join(self.docs_dir, "*.md"))
        chunk_id_counter = 0
        
        for file_path in md_files:
            filename = os.path.basename(file_path).replace(".md", "")
            with open(file_path, "r", encoding="utf-8") as f:
                content = f.read()
            
            # Simple splitting by sections (##)
            sections = content.split("\n## ")
            for i, section in enumerate(sections):
                if i > 0:
                    section = "## " + section # Add back the header marker for non-first chunks
                
                # Further split by paragraphs if too long? For now, keep sections.
                # Clean up empty lines
                lines = [line.strip() for line in section.split("\n") if line.strip()]
                if not lines:
                    continue
                
                chunk_text = "\n".join(lines)
                chunk_id = f"{filename}::chunk{i}"
                
                self.chunks.append({
                    "id": chunk_id,
                    "content": chunk_text,
                    "source": filename
                })

    def _build_index(self):
        if not self.chunks:
            return
        
        corpus = [chunk["content"] for chunk in self.chunks]
        self.vectorizer = TfidfVectorizer(stop_words='english')
        self.tfidf_matrix = self.vectorizer.fit_transform(corpus)

    def search(self, query: str, top_k: int = 3) -> List[Dict]:
        if not self.vectorizer or not self.chunks:
            return []

        query_vec = self.vectorizer.transform([query])
        similarities = cosine_similarity(query_vec, self.tfidf_matrix).flatten()
        
        # Get top k indices
        top_indices = similarities.argsort()[-top_k:][::-1]
        
        results = []
        for idx in top_indices:
            if similarities[idx] > 0: # Only return positive matches
                result = self.chunks[idx].copy()
                result["score"] = float(similarities[idx])
                results.append(result)
        
        return results
