import torch
from transformers import AutoTokenizer, AutoModelForSequenceClassification


class SentimentService:
    def __init__(self) -> None:
        self.tokenizer = AutoTokenizer.from_pretrained("ProsusAI/finbert")
        self.model = AutoModelForSequenceClassification.from_pretrained(
            "ProsusAI/finbert"
        )
        self.model.eval()
        self.id2label = self.model.config.id2label

    def analyze(self, text: str) -> dict:
        inputs = self.tokenizer(
            text, return_tensors="pt", truncation=True, padding=True
        )

        with torch.no_grad():
            outputs = self.model(**inputs)

        probs = torch.softmax(outputs.logits, dim=1)[0]
        idx = int(probs.argmax().item())

        return {
            "label": self.id2label[idx],
            "confidence": probs[idx].item(),
            "scores": {
                self.id2label[i]: probs[i].item() for i in range(len(self.id2label))
            },
        }
