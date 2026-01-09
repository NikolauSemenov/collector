package email

const HtmlBodyTpl = `
<!DOCTYPE html>
<html>
 <body style="margin:0;padding:0;background-color:#f4f6f8;">
   <table width="100%" cellpadding="0" cellspacing="0">
     <tr>
       <td align="center" style="padding:24px;">
         <table width="600" cellpadding="0" cellspacing="0" style="background:#ffffff;border-radius:8px;">
           <tr>
             <td style="padding:24px;font-family:Arial,sans-serif;">
               <h2 style="margin:0 0 16px;color:#333;">
                 {{.Header}}
               </h2>
               <p style="margin:0 0 16px;color:#555;font-size:14px;line-height:1.5;">
					{{.Text}}
               </p>
               <a href="https://example.com"
                  style="display:inline-block;
                         padding:12px 20px;
                         background:#2563eb;
                         color:#ffffff;
                         text-decoration:none;
                         border-radius:4px;
                         font-size:14px;">
                 Перейти
               </a>
               <p style="margin-top:24px;color:#999;font-size:12px;">
                 © 2025
               </p>
             </td>
           </tr>
         </table>
       </td>
     </tr>
   </table>
 </body>
</html>
`
