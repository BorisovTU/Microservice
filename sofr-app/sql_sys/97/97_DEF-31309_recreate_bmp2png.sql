BEGIN
  EXECUTE IMMEDIATE 'CREATE OR REPLACE AND RESOLVE JAVA SOURCE NAMED RSHB_RSPAYM_MB.BMP2PNG as import java.awt.Color;
  import java.awt.image.BufferedImage;
  import javax.imageio.ImageIO;
  import java.io.*;
  import oracle.sql.BLOB;
  import java.sql.*;

  class bmp2png
  {
    private static BLOB convertBMPToPNG(Color color, InputStream bmpInput) throws Exception
    {
           Connection con = null;
           BLOB OutBlob = null;
           BufferedImage input_image = null;

           con = DriverManager.getConnection("jdbc:default:connection:");
           OutBlob = BLOB.createTemporary(con, true, BLOB.DURATION_SESSION);
           OutputStream pngOutput = OutBlob.getBinaryOutputStream();

           input_image = ImageIO.read(bmpInput);
           BufferedImage image = new BufferedImage(input_image.getWidth(), input_image.getHeight(), BufferedImage.TYPE_INT_ARGB);

           image.getGraphics().drawImage(input_image, 0x00, 0x00, null);
           for (int i = 0x00; i < image.getWidth(); i++)
           {
                 for (int j = 0x00; j < image.getHeight(); j++)
                 {
                     if ((image.getRGB(i, j) | 0xFF000000) == (color.getRGB() | 0xFF000000))
                     {
                           image.setRGB(i, j, 0x00000000);
                     }
                 }
           }
           ImageIO.write(image, "PNG", pngOutput);
           return OutBlob;
    }

    public static BLOB convert(oracle.sql.BLOB InBlob) throws Exception
    {
           InputStream bmpInput = InBlob.getBinaryStream();
           BLOB OutBlob = convertBMPToPNG(new Color(255,0,255), bmpInput);
           return OutBlob;
    }
  }';
  
  EXCEPTION 
    WHEN OTHERS THEN DBMS_OUTPUT.PUT_LINE('Žè¨¡ª : '||SQLCODE||' - '||SQLERRM);
END;
/
