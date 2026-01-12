package email

const HtmlBodyTpl = `
<!DOCTYPE html>
<html>
	<body style="margin:0;padding:0;background-color:#f4f6f8;">
			<table width="100%" cellpadding="0" cellspacing="0" style="background-color:#f4f6f8;">
					<tr>
							<td align="center" style="padding:32px 16px;">
									<table width="600" cellpadding="0" cellspacing="0" style="max-width:600px;width:100%;background:#ffffff;border-radius:12px;overflow:hidden;box-shadow:0 10px 30px rgba(17,24,39,0.10);">
											<tr>
													<td style="padding:0;">
															<div style="height:6px;background:linear-gradient(90deg,#2563eb,#7c3aed,#06b6d4);line-height:6px;font-size:6px;">&nbsp;</div>
													</td>
											</tr>
											<tr>
													<td style="padding:28px 28px 20px;font-family:Arial,sans-serif;">
															<h2 style="margin:0 0 14px;color:#111827;font-size:20px;line-height:1.25;font-weight:700;">
																	{{.Header}}
															</h2>
															<div style="height:1px;background:#eef2f7;margin:0 0 18px;"></div>
															<p style="margin:0 0 12px;color:#374151;font-size:14px;line-height:1.6;">
					{{.Text}}
															</p>
															<p style="margin:18px 0 0;color:#6b7280;font-size:12px;line-height:1.5;">
																	Если вы не запрашивали это сообщение, просто проигнорируйте его.
															</p>
													</td>
											</tr>
											<tr>
													<td style="padding:14px 28px 24px;font-family:Arial,sans-serif;background:#fafbff;">
															<p style="margin:0;color:#9ca3af;font-size:12px;">
																	© 2026
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
