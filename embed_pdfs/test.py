from embed_pdf import embed_pdf
# Test
# embedding = embed_pdf("fake_path.csv")
embedding = embed_pdf("./data/pdf/0000320193-23-000005.pdf")
# torch.save(embedding, "data/colpali_embeddings/0000320193-23-000005.pt")