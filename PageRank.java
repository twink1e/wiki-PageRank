import java.io.*;
import java.nio.*;
import java.nio.channels.*;

class PageRank {
  private static final String LINK = "links.bin", SRC = "src.bin", DST = "dst.bin";
  private static final double DAMPING = 0.85;
  private static int numPage;

  private static void pageRankIter(int srcMem, int linkMem, int dstMem) {
    try {
      FileChannel linkIn = new FileInputStream(LINK).getChannel();
      FileChannel srcIn = new FileInputStream(SRC).getChannel();
      MappedByteBuffer link, src;

      link = linkIn.map(FileChannel.MapMode.READ_ONLY, 0, linkMem);
      link.getInt(); //discard the total page num
      int currPage = link.getInt(), currLink = link.getInt();
      int linkSize = linkIn.size(), linkOffset = linkMem;
      int linkDst;
      while(linkOffset < linkSize) {
        while(link.hasRemaining()){
          if (currPage == -1) currPage = link.getInt();
          else if (currLink == -1) currLink = link.getInt(); //necessary since page can have 0 link
          else if (currLink == 0) currLink = currPage = -1;
          else {
            linkDst = link.getInt();
            currLink--;
          }
        } 
        link = linkIn.map(FileChannel.MapMode.READ_ONLY, linkOffset, linkMem);
        linkOffset += linkMem;
      }
      
      mbb.getInt();
      mbb.position((int)in.size()-4);
      System.out.println(in.size());
      System.out.println(mbb.getInt());
      RandomAccessFile file = new RandomAccessFile(SRC, "rw");
      file.setLength(8 * numPage);
      int size = 5*1000000;
      BufferedOutputStream bos = new BufferedOutputStream(Channels.newOutputStream(file.getChannel()), size);
      DataOutputStream dos = new DataOutputStream(bos);
      in.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  /** All pages have initial score of 1/(number of pages) */
  private static void initSrc() {
    try {
      FileInputStream fis = new FileInputStream(LINK);
      BufferedInputStream bis = new BufferedInputStream(fis);
      DataInputStream dis = new DataInputStream(bis);

      FileOutputStream fos = new FileOutputStream(SRC);
      BufferedOutputStream bos = new BufferedOutputStream(fos);
      DataOutputStream dos = new DataOutputStream(bos);

      numPage = dis.readInt();
      dis.close();
      double score = 1.0/numPage;
      for(int i=0; i<numPage; i++) dos.writeDouble(score);
      dos.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  /** Create the dst file filled with 0*/
  private static void initDst() {
    try {
      FileOutputStream fos = new FileOutputStream(DST);
      BufferedOutputStream bos = new BufferedOutputStream(fos);
      DataOutputStream dos = new DataOutputStream(bos);
      double score = 0.0;
      for(int i=0; i<numPage; i++) dos.writeDouble(score);
      dos.close();
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  /** Delete SRC file, rename DST file to SRC*/
  private static void cleanUp() {
    try {
      File src = new File(SRC);
      File dst = new File(DST);
      src.delete();
      dst.renameTo(src);
    } catch (Exception e) {
      System.out.println(e);
    }
  }

  public static void main (String[] args) {
    if (args.length != 4) {
      System.out.println("Usage: java PageRank {iteration} {src MB} {link MB} {dst MB}");
      System.exit(-1);
    }
    int iter = Integer.parseInt(args[0]);
    int srcMem = Integer.parseInt(args[1]);
    int linkMem = Integer.parseInt(args[2]);
    int dstMem = Integer.parseInt(args[3]);
    long start, end, iterStart, iterEnd;
    start = System.currentTimeMillis();
    initSrc();
    end = System.currentTimeMillis();
    System.out.println("Init src took "+ (end - start) + " ms");
    start = System.currentTimeMillis();
    for (int i=0; i<iter; i++) {
      initDst();
      iterStart = System.currentTimeMillis();
      pageRankIter(srcMem * 1000000, linkMem * 1000000, dstMem * 1000000);
      iterEnd = System.currentTimeMillis();
      cleanUp();
      System.out.println("Iter " + i + " took "+ (iterEnd - iterStart) + " ms");
    }
    end = System.currentTimeMillis();
    System.out.println(iter + " iter " + srcMem + " MB src " + linkMem + " MB link " + dstMem + " MB dst took " + (end - start) + " ms");
  }
}