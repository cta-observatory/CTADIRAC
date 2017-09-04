void RootExample(const char* name, int stat) {

  std::cout<<"in RootExample"<<std::endl;

  TH1F* histo = new TH1F(name,name,100,-10,10);

  histo->FillRandom("gaus",stat);

  TCanvas* c = new TCanvas();

  histo->Draw();

  c->SaveAs("histo.ps");

}
