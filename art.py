import re

class ArticleSplitter:
    def __init__(self):
        self.basic_pattern = re.compile(r"\bArt\.\s+\d+", re.IGNORECASE)
        self.extended_pattern = re.compile(r"(?P<art>\bArt\.\s+\d+)(\s+Abs\.\s+\d+)?(\s+\b[A-Z][a-z]{2}\b)?", re.IGNORECASE)

    def split_sentences(self, text):
        # Split the text into sentences using periods followed by space as delimiters
        sentences_temp = re.split(r'\.\s+', text)
        
        # Since there's no direct manipulation of "Art." before this splitting, there's no need for placeholder replacement.
        sentences = sentences_temp
        return sentences

    def extract_article_contexts(self, text):
        sentences = self.split_sentences(text)
        article_contexts = []
        for i, sentence in enumerate(sentences):
            # Directly search for matches using the extended pattern within each sentence
            matches = [m.group() for m in self.extended_pattern.finditer(sentence)]
            if matches:
                context_start, context_end = max(0, i-2), min(i+2, len(sentences))
                context = " ".join(sentences[context_start:context_end])
                article_contexts.append((matches, context))
        return article_contexts

if __name__ == "__main__":
    text = ("Rechtsverletzungen hin (Art. 80 VRPG). 2. 2.1 Gegen Veranlagungsverfügungen kann innert 30 "
            "Tagen Einsprache erhoben werden (Art. 189 Abs 1 i.V.m. Art. 190 Abs 1 StG). Die vom Gesetz bestimmten "
            "Fristen können nicht erstreckt werden; bei Nichteinhaltung ist auf das verspätete Rechtsmittel "
            "grundsätzlich nicht einzutreten (Art. 161 Abs 1 StG; vgl. auch Art. 42 Abs 1 und Art. 43 Abs 1 VRPG "
            "[Umkehrschluss]). Ein Fristversäumnis wird jedoch entschuldigt, wenn die steuerpflichtige Person die "
            "versäumte Handlung innert dreissig Tagen seit Wegfall des Hinderungsgrunds nachholt und gleichzeitig "
            "nachweist, dass sie durch Militär- oder Zivildienst, Krankheit, Landesabwesenheit oder andere erhebliche "
            "Gründe an der rechtzeitigen Einreichung verhindert war (Art. 161 Abs 3 StG; vgl. auch Art. 43 Abs 2 VRPG).")
    splitter = ArticleSplitter()
    article_contexts = splitter.extract_article_contexts(text)
    for articles, context in article_contexts:
        print(f"Articles: {articles}, Context: {context}")
